"""
Python script to export data from MongoDB Atlas directly to Snowflake.
MongoDB documents are stored as VARIANT data type in Snowflake tables.

Optimized for parallel processing with:
- Concurrent batch insertions using ThreadPoolExecutor
- Producer-consumer pattern with queues
- Multiple Snowflake connections for parallel inserts
"""

import os
import json
import argparse
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Lock, Thread
import time


def get_document_hash(doc: Dict[str, Any]) -> str:
    """
    Generate MD5 hash of document content for change detection.
    
    Args:
        doc: Dictionary representing the MongoDB document
        
    Returns:
        MD5 hash string of the document
    """
    doc_str = json.dumps(doc, sort_keys=True, default=str)
    return hashlib.md5(doc_str.encode()).hexdigest()


class MongoAtlasSnowflakeExport:
    """Class to handle MongoDB Atlas to Snowflake data export.
    
    This class connects to MongoDB Atlas, reads collections, and exports
    the data directly to Snowflake VARIANT columns.
    
    Optimized for parallel processing with configurable worker threads.
    """
    
    def __init__(self, mongo_config, snowflake_config, num_workers: int = 4):
        """
        Initialize MongoDB and Snowflake connections.
        
        Args:
            mongo_config (dict): Dictionary containing MongoDB connection parameters:
                - connection_string: MongoDB Atlas connection string
                - database: MongoDB database name
            snowflake_config (dict): Dictionary containing Snowflake connection parameters:
                - account: Snowflake account identifier
                - user: Snowflake username
                - password: Snowflake password (or use authenticator)
                - warehouse: Snowflake warehouse name
                - database: Snowflake database name
                - schema: Snowflake schema name
                - role: Snowflake role (optional)
            num_workers (int): Number of parallel worker threads for batch insertion (default: 4)
        """
        self.mongo_config = mongo_config
        self.snowflake_config = snowflake_config
        self.num_workers = num_workers
        self.mongo_client = None
        self.mongo_db = None
        self.snowflake_conn = None
        self.snowflake_cursor = None
        
        # Thread-safe counters and locks for parallel processing
        self._insert_lock = Lock()
        self._stats_lock = Lock()
        self._print_lock = Lock()
        
        # Connection pool for parallel inserts
        self._connection_pool: List[Any] = []
        self._pool_lock = Lock()
        
    @staticmethod
    def _decode_private_key(key_content, passphrase=''):
        """Decode a base64-encoded private key and return DER bytes for Snowflake."""
        import base64
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        try:
            key_data = base64.b64decode(key_content)
        except Exception:
            key_data = key_content.encode('utf-8') if isinstance(key_content, str) else key_content

        p_key = serialization.load_pem_private_key(
            key_data,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

    def connect_mongo(self):
        """Establish connection to MongoDB Atlas."""
        try:
            connection_string = self.mongo_config['connection_string']
            database_name = self.mongo_config['database']
            
            print(f"Connecting to MongoDB Atlas...")
            print(f"Database: {database_name}")
            
            self.mongo_client = MongoClient(connection_string)
            
            # Test connection
            self.mongo_client.admin.command('ping')
            
            self.mongo_db = self.mongo_client[database_name]
            
            print(f"Successfully connected to MongoDB Atlas (Database: {database_name})")
        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB Atlas: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error connecting to MongoDB: {e}")
            raise
    
    def connect_snowflake(self):
        """Establish connection to Snowflake."""
        try:
            print(f"Connecting to Snowflake...")
            print(f"Account: {self.snowflake_config['account']}")
            print(f"Database: {self.snowflake_config['database']}")
            print(f"Schema: {self.snowflake_config['schema']}")
            
            # Build connection parameters
            conn_params = {
                'account': self.snowflake_config['account'],
                'user': self.snowflake_config['user'],
                'warehouse': self.snowflake_config['warehouse'],
                'database': self.snowflake_config['database'],
                'schema': self.snowflake_config['schema'],
                'role': self.snowflake_config.get('role'),
            }
            
            # Use private key authentication if provided, otherwise use password
            private_key_path = self.snowflake_config.get('private_key_path')
            private_key_content = self.snowflake_config.get('private_key')
            if private_key_path:
                print(f"Using private key authentication from file: {private_key_path}")
                conn_params['private_key_file'] = private_key_path
                if self.snowflake_config.get('private_key_passphrase'):
                    passphrase = self.snowflake_config['private_key_passphrase']
                    conn_params['private_key_file_pwd'] = passphrase.encode() if isinstance(passphrase, str) else passphrase
            elif private_key_content:
                print(f"Using private key authentication from inline content")
                conn_params['private_key'] = self._decode_private_key(
                    private_key_content, self.snowflake_config.get('private_key_passphrase', ''))
            else:
                conn_params['password'] = self.snowflake_config.get('password')
                conn_params['authenticator'] = self.snowflake_config.get('authenticator', 'snowflake')
            
            self.snowflake_conn = connect(**conn_params)
            self.snowflake_cursor = self.snowflake_conn.cursor()
            
            # Explicitly set database and schema to ensure session context is set
            if self.snowflake_config.get('database'):
                db_name = self.snowflake_config['database']
                try:
                    self.snowflake_cursor.execute(f'USE DATABASE {db_name}')
                    print(f"Set database context to: {db_name}")
                except ProgrammingError as e:
                    try:
                        self.snowflake_cursor.execute(f'USE DATABASE "{db_name}"')
                        print(f"Set database context to: {db_name} (quoted)")
                    except ProgrammingError as e2:
                        print(f"Error: Could not set database context: {e2}")
                        raise
            
            if self.snowflake_config.get('schema'):
                schema_name = self.snowflake_config['schema']
                try:
                    self.snowflake_cursor.execute(f'USE SCHEMA {schema_name}')
                    print(f"Set schema context to: {schema_name}")
                except ProgrammingError as e:
                    try:
                        self.snowflake_cursor.execute(f'USE SCHEMA "{schema_name}"')
                        print(f"Set schema context to: {schema_name} (quoted)")
                    except ProgrammingError as e2:
                        print(f"Warning: Could not set schema context: {e2}")
            
            print(f"Successfully connected to Snowflake (Database: {self.snowflake_config.get('database')}, Schema: {self.snowflake_config.get('schema')})")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise
    
    def _create_snowflake_connection(self):
        """Create a new Snowflake connection for the connection pool."""
        conn_params = {
            'account': self.snowflake_config['account'],
            'user': self.snowflake_config['user'],
            'warehouse': self.snowflake_config['warehouse'],
            'database': self.snowflake_config['database'],
            'schema': self.snowflake_config['schema'],
            'role': self.snowflake_config.get('role'),
        }
        
        private_key_path = self.snowflake_config.get('private_key_path')
        private_key_content = self.snowflake_config.get('private_key')
        if private_key_path:
            conn_params['private_key_file'] = private_key_path
            if self.snowflake_config.get('private_key_passphrase'):
                passphrase = self.snowflake_config['private_key_passphrase']
                conn_params['private_key_file_pwd'] = passphrase.encode() if isinstance(passphrase, str) else passphrase
        elif private_key_content:
            conn_params['private_key'] = self._decode_private_key(
                private_key_content, self.snowflake_config.get('private_key_passphrase', ''))
        else:
            conn_params['password'] = self.snowflake_config.get('password')
            conn_params['authenticator'] = self.snowflake_config.get('authenticator', 'snowflake')
        
        conn = connect(**conn_params)
        cursor = conn.cursor()
        
        # Set database and schema context
        if self.snowflake_config.get('database'):
            try:
                cursor.execute(f"USE DATABASE {self.snowflake_config['database']}")
            except ProgrammingError:
                cursor.execute(f'USE DATABASE "{self.snowflake_config["database"]}"')
        
        if self.snowflake_config.get('schema'):
            try:
                cursor.execute(f"USE SCHEMA {self.snowflake_config['schema']}")
            except ProgrammingError:
                cursor.execute(f'USE SCHEMA "{self.snowflake_config["schema"]}"')
        
        return conn, cursor
    
    def _init_connection_pool(self):
        """Initialize connection pool for parallel inserts."""
        print(f"Initializing connection pool with {self.num_workers} connections...")
        for i in range(self.num_workers):
            try:
                conn, cursor = self._create_snowflake_connection()
                self._connection_pool.append({'conn': conn, 'cursor': cursor, 'in_use': False})
            except Exception as e:
                print(f"Warning: Failed to create connection {i+1}: {e}")
        print(f"Connection pool ready with {len(self._connection_pool)} connections")
    
    def _get_connection(self):
        """Get an available connection from the pool."""
        with self._pool_lock:
            for conn_info in self._connection_pool:
                if not conn_info['in_use']:
                    conn_info['in_use'] = True
                    return conn_info
        return None
    
    def _release_connection(self, conn_info):
        """Release a connection back to the pool."""
        with self._pool_lock:
            conn_info['in_use'] = False
    
    def _close_connection_pool(self):
        """Close all connections in the pool."""
        for conn_info in self._connection_pool:
            try:
                if conn_info['cursor']:
                    conn_info['cursor'].close()
                if conn_info['conn']:
                    conn_info['conn'].close()
            except Exception:
                pass
        self._connection_pool.clear()
    
    def _thread_safe_print(self, message: str):
        """Thread-safe print function."""
        with self._print_lock:
            print(message)
    
    def list_collections(self):
        """
        List all collections in the MongoDB database.
        
        Returns:
            list: List of collection names
        """
        try:
            collections = self.mongo_db.list_collection_names()
            print(f"\nFound {len(collections)} collection(s) in database:")
            for collection in collections:
                count = self.mongo_db[collection].count_documents({})
                print(f"  - {collection} ({count} documents)")
            return collections
        except Exception as e:
            print(f"Error listing collections: {e}")
            raise
    
    def create_target_table(self, table_name):
        """
        Create a target table with VARIANT column to store MongoDB documents as JSON.
        Only creates the table if it doesn't exist (insert-only mode).
        
        Table structure:
        - ROW_ID: Auto-incrementing row identifier
        - ROW_DATA: VARIANT column storing each MongoDB document as JSON
        - DOCUMENT_ID: STRING computed column extracting MongoDB _id from ROW_DATA
        - COLLECTION_NAME: STRING column storing the source MongoDB collection name
        - LOAD_TIMESTAMP: Timestamp of when the row was loaded
        
        Clustering:
        - Table is clustered by (LOAD_TIMESTAMP, DOCUMENT_ID) for optimized query performance
        
        Args:
            table_name (str): Name of the target table
        """
        try:
            print(f"\n=== Creating Target Table ===")
            print(f"Table Name: {table_name}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
                ROW_DATA VARIANT,
                DOCUMENT_ID STRING AS (COALESCE(ROW_DATA:"_id"::STRING, ROW_DATA:"_id"."$oid"::STRING)),
                DOCUMENT_HASH STRING,
                COLLECTION_NAME STRING,
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (LOAD_TIMESTAMP, DOCUMENT_ID)
            """
            
            print(f"\nCREATE TABLE SQL:")
            print(create_table_sql)
            print("=" * 40)
            
            self.snowflake_cursor.execute(create_table_sql)
            print(f"Table '{table_name}' ready (created if not exists)")
        except ProgrammingError as e:
            print(f"Error creating table: {e}")
            raise
    
    def _extract_fields(self, doc: Dict[str, Any], fields: Optional[List[str]]) -> Dict[str, Any]:
        """
        Extract only specified fields from a MongoDB document.
        Supports nested fields using dot notation (e.g., 'user.name', 'address.city').
        
        Args:
            doc (dict): MongoDB document
            fields (list): List of field names to extract. If None, returns entire document.
                          Supports dot notation for nested fields (e.g., 'user.name').
                          If a parent field is specified (e.g., 'user'), the entire object is included.
                          If nested fields are specified (e.g., 'user.name'), only those nested fields are included.
        
        Returns:
            dict: Document containing only the specified fields
        """
        if fields is None:
            return doc
        
        result = {}
        # Track which parent fields are included entirely (not just nested sub-fields)
        parent_fields_included = set()
        
        # First pass: identify parent fields that are included entirely
        for field_path in fields:
            if '.' not in field_path and field_path in doc:
                parent_fields_included.add(field_path)
                result[field_path] = doc[field_path]
        
        # Second pass: handle nested fields
        for field_path in fields:
            # Handle nested fields with dot notation
            if '.' in field_path:
                parts = field_path.split('.')
                parent_field = parts[0]
                
                # If parent field is already included entirely, skip nested extraction
                if parent_field in parent_fields_included:
                    continue
                
                # Navigate through nested structure to get the value
                value = doc
                try:
                    for part in parts:
                        if isinstance(value, dict) and part in value:
                            value = value[part]
                        else:
                            value = None
                            break
                    
                    # Set nested value in result, creating parent structure if needed
                    if value is not None:
                        if parent_field not in result:
                            result[parent_field] = {}
                        current = result[parent_field]
                        
                        # Navigate/create nested structure
                        for part in parts[1:-1]:
                            if part not in current or not isinstance(current[part], dict):
                                current[part] = {}
                            current = current[part]
                        current[parts[-1]] = value
                except (TypeError, KeyError, AttributeError):
                    # Field path doesn't exist or structure is invalid, skip it
                    pass
        
        # Always include _id if it exists in the original document
        if '_id' in doc and '_id' not in result:
            result['_id'] = doc['_id']
        
        return result
    
    def export_collection_to_snowflake(
        self,
        collection_name: str,
        table_name: str,
        query: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 1000,
        upsert: bool = False,
        parallel: bool = True,
        dry_run: bool = False,
        limit: Optional[int] = None
    ):
        """
        Export a MongoDB collection directly to Snowflake VARIANT column.
        
        Args:
            collection_name (str): Name of the MongoDB collection to export
            table_name (str): Name of the Snowflake target table
            query (dict): MongoDB query filter (optional, exports all documents if not provided)
            fields (list): Optional list of field names to extract from documents.
                          If None, exports entire documents.
                          Supports dot notation for nested fields (e.g., ['name', 'email', 'address.city']).
                          The '_id' field is always included if present.
            batch_size (int): Number of documents to insert per batch (default: 1000)
            upsert (bool): If True, update existing records based on DOCUMENT_ID (_id).
                          If False, only insert new records (default: False)
            parallel (bool): If True, use parallel batch processing (default: True)
            dry_run (bool): If True, only show what would be done without inserting data (default: False)
        
        Returns:
            int: Number of documents processed (inserted + updated if upsert=True)
        """
        if dry_run:
            return self._dry_run_export(collection_name, table_name, query, fields, batch_size, upsert, limit)
        elif parallel and not upsert:
            return self._export_collection_parallel(
                collection_name, table_name, query, fields, batch_size, limit
            )
        else:
            return self._export_collection_sequential(
                collection_name, table_name, query, fields, batch_size, upsert, limit
            )
    
    def _dry_run_export(
        self,
        collection_name: str,
        table_name: str,
        query: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 1000,
        upsert: bool = False,
        limit: Optional[int] = None
    ):
        """
        Dry run mode: Show what would be done without actually inserting data.
        """
        try:
            collection = self.mongo_db[collection_name]
            
            print(f"\n{'=' * 60}")
            print(f"=== DRY RUN MODE - No data will be inserted ===")
            print(f"{'=' * 60}")
            print(f"\nMongoDB Collection: {collection_name}")
            print(f"Snowflake Table: {table_name}")
            print(f"Query: {query if query else 'All documents'}")
            if fields:
                print(f"Fields to extract: {', '.join(fields)}")
            else:
                print(f"Fields to extract: All fields")
            print(f"Batch Size: {batch_size:,}")
            print(f"Upsert Mode: {'Enabled' if upsert else 'Disabled'}")
            print(f"Document Limit: {limit:,}" if limit else "Document Limit: No limit")
            
            # Get document count
            total_docs = collection.count_documents(query if query else {})
            docs_to_export = min(total_docs, limit) if limit else total_docs
            print(f"\n--- Document Analysis ---")
            print(f"Total documents matching query: {total_docs:,}")
            if limit and total_docs > limit:
                print(f"Documents to export (with limit): {docs_to_export:,}")
            
            if total_docs == 0:
                print("No documents to export")
                return 0
            
            # Calculate batches
            num_batches = (total_docs + batch_size - 1) // batch_size
            print(f"Number of batches: {num_batches:,}")
            
            # Check if table exists and get row count
            print(f"\n--- Snowflake Table Analysis ---")
            try:
                check_sql = f"SELECT COUNT(*) FROM {table_name}"
                self.snowflake_cursor.execute(check_sql)
                existing_count = self.snowflake_cursor.fetchone()[0]
                print(f"Table '{table_name}' exists")
                print(f"Current row count: {existing_count:,}")
                
                # Check existing documents for this collection
                collection_sql = f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE COLLECTION_NAME = '{collection_name.replace("'", "''")}'
                """
                self.snowflake_cursor.execute(collection_sql)
                collection_count = self.snowflake_cursor.fetchone()[0]
                print(f"Rows for collection '{collection_name}': {collection_count:,}")
                
                if upsert:
                    # Check how many would be updates vs inserts
                    doc_ids_sql = f"""
                    SELECT COUNT(DISTINCT DOCUMENT_ID) FROM {table_name}
                    WHERE COLLECTION_NAME = '{collection_name.replace("'", "''")}'
                    """
                    self.snowflake_cursor.execute(doc_ids_sql)
                    existing_doc_ids_count = self.snowflake_cursor.fetchone()[0]
                    print(f"Existing unique document IDs: {existing_doc_ids_count:,}")
                    print(f"Potential updates: up to {min(existing_doc_ids_count, total_docs):,}")
                    print(f"Potential new inserts: {max(0, total_docs - existing_doc_ids_count):,}")
                    
            except ProgrammingError as e:
                if "does not exist" in str(e).lower() or "Object" in str(e):
                    print(f"Table '{table_name}' does not exist (will be created)")
                else:
                    print(f"Could not check table: {e}")
            
            # Sample documents
            print(f"\n--- Sample Documents (first 3) ---")
            sample_cursor = collection.find(query if query else {}).limit(3)
            for i, doc in enumerate(sample_cursor, 1):
                if fields:
                    doc = self._extract_fields(doc, fields)
                
                # Convert ObjectId
                if '_id' in doc and hasattr(doc['_id'], '__str__'):
                    doc['_id'] = str(doc['_id'])
                
                # Show sample
                doc_json = json.dumps(doc, default=str, indent=2)
                if len(doc_json) > 500:
                    doc_json = doc_json[:500] + "\n  ... (truncated)"
                print(f"\nDocument {i}:")
                print(doc_json)
            
            # Estimate size
            print(f"\n--- Size Estimate ---")
            sample_sizes = []
            size_sample = collection.find(query if query else {}).limit(100)
            for doc in size_sample:
                if fields:
                    doc = self._extract_fields(doc, fields)
                if '_id' in doc and hasattr(doc['_id'], '__str__'):
                    doc['_id'] = str(doc['_id'])
                doc_json = json.dumps(doc, default=str)
                sample_sizes.append(len(doc_json))
            
            if sample_sizes:
                avg_size = sum(sample_sizes) / len(sample_sizes)
                estimated_total = avg_size * total_docs
                print(f"Average document size: {avg_size:,.0f} bytes")
                print(f"Estimated total data size: {estimated_total / (1024*1024):,.1f} MB")
            
            print(f"\n{'=' * 60}")
            print(f"=== DRY RUN COMPLETE ===")
            print(f"{'=' * 60}")
            print(f"\nTo execute the actual export, set dry_run = False")
            
            return 0
            
        except Exception as e:
            print(f"Error in dry run: {e}")
            raise
    
    def _export_collection_parallel(
        self,
        collection_name: str,
        table_name: str,
        query: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 1000,
        limit: Optional[int] = None
    ):
        """
        Parallel export using multiple threads and connection pool.
        Optimized for high-throughput INSERT operations.
        """
        try:
            collection = self.mongo_db[collection_name]
            
            print(f"\n=== Parallel Export to Snowflake ===")
            print(f"MongoDB Collection: {collection_name}")
            print(f"Snowflake Table: {table_name}")
            print(f"Query: {query if query else 'All documents'}")
            print(f"Workers: {self.num_workers}")
            print(f"Batch Size: {batch_size:,}")
            print(f"Document Limit: {limit:,}" if limit else "Document Limit: No limit")
            if fields:
                print(f"Fields to extract: {', '.join(fields)}")
            else:
                print(f"Fields to extract: All fields")
            print("=" * 40)
            
            # Get total count
            total_in_collection = collection.count_documents(query if query else {})
            total_docs = min(total_in_collection, limit) if limit else total_in_collection
            print(f"Total documents matching query: {total_in_collection:,}")
            if limit and total_in_collection > limit:
                print(f"Documents to export (with limit): {total_docs:,}")
            
            if total_docs == 0:
                print("No documents to export")
                return 0
            
            # Initialize connection pool for parallel inserts
            self._init_connection_pool()
            
            # Shared state for parallel processing
            total_inserted = 0
            all_failed_documents = []
            batch_queue = Queue(maxsize=self.num_workers * 2)  # Buffer batches
            producer_done = {'done': False}
            start_time = time.time()
            
            def process_batch_worker():
                """Worker thread that processes batches from the queue."""
                nonlocal total_inserted
                worker_inserted = 0
                worker_failed = []
                
                while True:
                    try:
                        batch_info = batch_queue.get(timeout=1)
                        if batch_info is None:  # Poison pill
                            break
                        
                        batch_docs, batch_num = batch_info
                        conn_info = None
                        
                        try:
                            # Get connection from pool (with retry)
                            for _ in range(10):
                                conn_info = self._get_connection()
                                if conn_info:
                                    break
                                time.sleep(0.1)
                            
                            if not conn_info:
                                # Fall back to main connection
                                inserted, _, failed = self._insert_batch_to_snowflake(
                                    batch_docs, collection_name, table_name, batch_num, False, set()
                                )
                            else:
                                # Use pooled connection
                                inserted, failed = self._insert_batch_parallel(
                                    batch_docs, collection_name, table_name, 
                                    conn_info['cursor'], batch_num
                                )
                            
                            with self._stats_lock:
                                worker_inserted += inserted
                                worker_failed.extend(failed)
                            
                            self._thread_safe_print(
                                f"   Batch {batch_num}: {inserted:,} inserted, {len(failed)} failed"
                            )
                            
                        finally:
                            if conn_info:
                                self._release_connection(conn_info)
                            batch_queue.task_done()
                            
                    except Empty:
                        if producer_done['done']:
                            break
                        continue
                    except Exception as e:
                        self._thread_safe_print(f"   Worker error: {e}")
                        try:
                            batch_queue.task_done()
                        except:
                            pass
                
                # Update totals
                with self._stats_lock:
                    nonlocal total_inserted, all_failed_documents
                    total_inserted += worker_inserted
                    all_failed_documents.extend(worker_failed)
            
            # Start worker threads
            workers = []
            for _ in range(self.num_workers):
                t = Thread(target=process_batch_worker, daemon=True)
                t.start()
                workers.append(t)
            
            print(f"\nStarted {self.num_workers} worker threads")
            print("Processing documents...")
            
            # Producer: fetch documents and create batches
            cursor = collection.find(query if query else {})
            if limit:
                cursor = cursor.limit(limit)
            batch_docs = []
            batch_num = 0
            processed = 0
            last_progress = 0
            
            for doc in cursor:
                processed += 1
                
                # Show progress every 5%
                progress_pct = (processed * 100) // total_docs if total_docs > 0 else 0
                if progress_pct >= last_progress + 5:
                    elapsed = time.time() - start_time
                    docs_per_sec = processed / elapsed if elapsed > 0 else 0
                    eta = (total_docs - processed) / docs_per_sec if docs_per_sec > 0 else 0
                    print(f"   Progress: {processed:,} / {total_docs:,} ({progress_pct}%) "
                          f"| {docs_per_sec:,.0f} docs/sec | ETA: {eta:.0f}s")
                    last_progress = progress_pct
                
                # Extract fields if specified
                if fields:
                    doc = self._extract_fields(doc, fields)
                
                # Convert ObjectId to string
                if '_id' in doc:
                    if hasattr(doc['_id'], '__str__'):
                        doc['_id'] = str(doc['_id'])
                
                batch_docs.append(doc)
                
                # Queue batch for processing
                if len(batch_docs) >= batch_size:
                    batch_num += 1
                    batch_queue.put((batch_docs.copy(), batch_num))
                    batch_docs = []
            
            # Queue final batch
            if batch_docs:
                batch_num += 1
                batch_queue.put((batch_docs.copy(), batch_num))
            
            # Signal workers to finish
            producer_done['done'] = True
            for _ in range(self.num_workers):
                batch_queue.put(None)  # Poison pills
            
            # Wait for all workers to complete
            for t in workers:
                t.join(timeout=300)  # 5 minute timeout per worker
            
            # Close connection pool
            self._close_connection_pool()
            
            elapsed = time.time() - start_time
            docs_per_sec = total_inserted / elapsed if elapsed > 0 else 0
            
            # Summary
            print(f"\n{'=' * 60}")
            print(f"=== Parallel Export Summary ===")
            print(f"{'=' * 60}")
            print(f"Total documents processed: {total_docs:,}")
            print(f"Successfully inserted: {total_inserted:,} document(s)")
            print(f"Failed: {len(all_failed_documents):,} document(s)")
            print(f"Time elapsed: {elapsed:.1f} seconds")
            print(f"Throughput: {docs_per_sec:,.0f} documents/second")
            
            if total_inserted == 0 and total_docs > 0:
                raise Exception(f"Failed to insert any documents. All {total_docs} documents failed.")
            
            self._print_failure_report(all_failed_documents)
            
            print(f"\n{'=' * 60}")
            return total_inserted
            
        except Exception as e:
            self._close_connection_pool()
            print(f"Error in parallel export: {e}")
            raise
    
    def _insert_batch_parallel(
        self,
        documents: List[Dict],
        collection_name: str,
        table_name: str,
        cursor,
        batch_num: int
    ):
        """
        Insert a batch of documents using a specific cursor (for parallel processing).
        Optimized for speed with minimal overhead.
        """
        if not documents:
            return 0, []
        
        rows_inserted = 0
        failed_documents = []
        collection_name_escaped = collection_name.replace("'", "''")
        insert_chunk_size = 500  # Optimal chunk size for UNION ALL inserts
        
        # Prepare all documents first
        prepared_docs = []
        for doc in documents:
            doc_id = str(doc.get('_id', 'unknown')) if doc.get('_id') else 'unknown'
            try:
                cleaned_doc = self._clean_document(doc)
                json_str = json.dumps(cleaned_doc, default=str, ensure_ascii=False, separators=(',', ':'))
                json.loads(json_str)  # Validate
                doc_hash = get_document_hash(cleaned_doc)
                prepared_docs.append({'doc_id': doc_id, 'json_str': json_str, 'doc_hash': doc_hash})
            except Exception as e:
                failed_documents.append({
                    'doc_id': doc_id,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'json_sample': None
                })
        
        # Insert in chunks using UNION ALL
        for i in range(0, len(prepared_docs), insert_chunk_size):
            chunk = prepared_docs[i:i + insert_chunk_size]
            
            try:
                select_clauses = []
                params = []
                
                for item in chunk:
                    select_clauses.append(f"SELECT PARSE_JSON(%s) AS ROW_DATA, %s AS DOCUMENT_HASH, %s AS COLLECTION_NAME")
                    params.extend([item['json_str'], item.get('doc_hash', ''), collection_name])
                
                insert_sql = f"""
                INSERT INTO {table_name} (ROW_DATA, DOCUMENT_HASH, COLLECTION_NAME)
                {' UNION ALL '.join(select_clauses)}
                """
                
                cursor.execute(insert_sql, params)
                rows_inserted += len(chunk)
                
            except Exception as e:
                # Fallback to individual inserts for this chunk
                for item in chunk:
                    try:
                        doc_hash = item.get('doc_hash', '')
                        insert_sql = f"""
                        INSERT INTO {table_name} (ROW_DATA, DOCUMENT_HASH, COLLECTION_NAME)
                        SELECT PARSE_JSON(%s), %s, '{collection_name_escaped}'
                        """
                        cursor.execute(insert_sql, [item['json_str'], doc_hash])
                        rows_inserted += 1
                    except Exception as e2:
                        failed_documents.append({
                            'doc_id': item['doc_id'],
                            'error_type': type(e2).__name__,
                            'error_message': str(e2),
                            'json_sample': item['json_str'][:200] if item['json_str'] else None
                        })
        
        return rows_inserted, failed_documents
    
    def _print_failure_report(self, all_failed_documents: List[Dict]):
        """Print detailed failure report."""
        if not all_failed_documents:
            return
        
        print(f"\n{'=' * 60}")
        print(f"=== Failed Documents Report ===")
        print(f"{'=' * 60}")
        
        # Group by error type
        error_types = {}
        for failed in all_failed_documents:
            error_type = failed['error_type']
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append(failed)
        
        print(f"\nFailures by error type:")
        for error_type, failures in sorted(error_types.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  - {error_type}: {len(failures)} document(s)")
        
        print(f"\nDetailed error examples:")
        for error_type, failures in sorted(error_types.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"\n  {error_type} ({len(failures)} occurrence(s)):")
            for i, failed in enumerate(failures[:3], 1):
                print(f"    Example {i}:")
                print(f"      Document ID: {failed['doc_id']}")
                print(f"      Error: {failed['error_message'][:400]}")
            if len(failures) > 3:
                print(f"    ... and {len(failures) - 3} more")
    
    def _export_collection_sequential(
        self,
        collection_name: str,
        table_name: str,
        query: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 1000,
        upsert: bool = False,
        limit: Optional[int] = None
    ):
        """
        Sequential export (original implementation, used for upsert mode).
        """
        try:
            collection = self.mongo_db[collection_name]
            
            print(f"\n=== Exporting Collection to Snowflake (Sequential) ===")
            print(f"MongoDB Collection: {collection_name}")
            print(f"Snowflake Table: {table_name}")
            print(f"Query: {query if query else 'All documents'}")
            print(f"Document Limit: {limit:,}" if limit else "Document Limit: No limit")
            if fields:
                print(f"Fields to extract: {', '.join(fields)}")
            else:
                print(f"Fields to extract: All fields")
            print("=" * 40)
            
            # Get total count
            total_in_collection = collection.count_documents(query if query else {})
            total_docs = min(total_in_collection, limit) if limit else total_in_collection
            print(f"Total documents matching query: {total_in_collection:,}")
            if limit and total_in_collection > limit:
                print(f"Documents to export (with limit): {total_docs:,}")
            
            if total_docs == 0:
                print("No documents to export")
                return 0
            
            # If upsert mode, check for existing document IDs
            existing_doc_ids = set()
            if upsert:
                print(f"\n=== Checking Existing Documents ===")
                try:
                    check_sql = f"""
                    SELECT DISTINCT DOCUMENT_ID
                    FROM {table_name}
                    WHERE COLLECTION_NAME = '{collection_name.replace("'", "''")}'
                    """
                    self.snowflake_cursor.execute(check_sql)
                    existing_doc_ids = {row[0] for row in self.snowflake_cursor.fetchall() if row[0]}
                    print(f"Found {len(existing_doc_ids)} existing document(s) in target table")
                    print(f"Mode: UPSERT (will update existing, insert new)")
                except ProgrammingError as e:
                    print(f"Note: Could not check existing documents (table may be empty): {e}")
                    existing_doc_ids = set()
                    print(f"Mode: UPSERT (assuming no existing documents)")
            else:
                print(f"Mode: INSERT ONLY (will skip duplicates)")
            
            # Fetch documents in batches
            print("Fetching documents from MongoDB...")
            cursor = collection.find(query if query else {})
            if limit:
                cursor = cursor.limit(limit)
            
            batch_docs = []
            batch_num = 0
            total_inserted = 0
            total_updated = 0
            all_failed_documents = []
            processed = 0
            start_time = time.time()
            
            print("Processing documents...")
            for doc in cursor:
                processed += 1
                
                # Show progress every 1000 documents
                if processed % 1000 == 0:
                    elapsed = time.time() - start_time
                    docs_per_sec = processed / elapsed if elapsed > 0 else 0
                    print(f"   Processed: {processed:,} / {total_docs:,} ({processed*100//total_docs if total_docs > 0 else 0}%) | {docs_per_sec:,.0f} docs/sec")
                
                if fields:
                    doc = self._extract_fields(doc, fields)
                
                if '_id' in doc:
                    if hasattr(doc['_id'], '__str__'):
                        doc['_id'] = str(doc['_id'])
                
                batch_docs.append(doc)
                
                if len(batch_docs) >= batch_size:
                    batch_num += 1
                    action = "Upserting" if upsert else "Inserting"
                    print(f"   {action} batch {batch_num} ({len(batch_docs)} documents)...")
                    inserted, updated, failed = self._insert_batch_to_snowflake(
                        batch_docs, collection_name, table_name, batch_num, upsert, existing_doc_ids
                    )
                    total_inserted += inserted
                    total_updated += updated
                    all_failed_documents.extend(failed)
                    if upsert:
                        print(f"   Batch {batch_num} completed: {inserted} inserted, {updated} updated, {len(failed)} failed")
                    else:
                        print(f"   Batch {batch_num} completed: {inserted} inserted, {len(failed)} failed")
                    batch_docs = []
            
            if batch_docs:
                batch_num += 1
                action = "Upserting" if upsert else "Inserting"
                print(f"   {action} final batch {batch_num} ({len(batch_docs)} documents)...")
                inserted, updated, failed = self._insert_batch_to_snowflake(
                    batch_docs, collection_name, table_name, batch_num, upsert, existing_doc_ids
                )
                total_inserted += inserted
                total_updated += updated
                all_failed_documents.extend(failed)
                if upsert:
                    print(f"   Batch {batch_num} completed: {inserted} inserted, {updated} updated, {len(failed)} failed")
                else:
                    print(f"   Batch {batch_num} completed: {inserted} inserted, {len(failed)} failed")
            
            elapsed = time.time() - start_time
            docs_per_sec = (total_inserted + total_updated) / elapsed if elapsed > 0 else 0
            
            print(f"\n{'=' * 60}")
            print(f"=== Export Summary ===")
            print(f"{'=' * 60}")
            print(f"Total documents processed: {total_docs:,}")
            print(f"Successfully inserted: {total_inserted:,} document(s)")
            if upsert:
                print(f"Successfully updated: {total_updated:,} document(s)")
            print(f"Failed: {len(all_failed_documents):,} document(s)")
            print(f"Time elapsed: {elapsed:.1f} seconds")
            print(f"Throughput: {docs_per_sec:,.0f} documents/second")
            
            total_processed = total_inserted + (total_updated if upsert else 0)
            if total_processed == 0 and total_docs > 0:
                raise Exception(f"Failed to insert any documents. All {total_docs} documents failed.")
            
            self._print_failure_report(all_failed_documents)
            
            print(f"\n{'=' * 60}")
            return total_processed
            
        except Exception as e:
            print(f"Error exporting collection to Snowflake: {e}")
            raise
    
    def _clean_document(self, obj):
        """
        Recursively clean document to ensure valid JSON.
        Leaves nested JSON strings as-is (don't parse them) to avoid double-escaping issues.
        
        Args:
            obj: The object to process (dict, list, or value)
        
        Returns:
            Cleaned object ready for JSON serialization
        """
        if isinstance(obj, dict):
            return {k: self._clean_document(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_document(item) for item in obj]
        elif isinstance(obj, str):
            # Leave strings as-is - don't try to parse nested JSON strings
            # This avoids double-escaping issues with PARSE_JSON
            return obj
        elif isinstance(obj, (int, float, bool)) or obj is None:
            return obj
        else:
            # Convert other types to string
            return str(obj)
    
    def _insert_batch_to_snowflake(
        self, 
        documents: List[Dict], 
        collection_name: str, 
        table_name: str, 
        batch_num: int,
        upsert: bool = False,
        existing_doc_ids: set = None
    ):
        """
        Insert or update a batch of MongoDB documents into Snowflake.
        
        Args:
            documents (list): List of MongoDB documents to insert/update
            collection_name (str): Name of the MongoDB collection
            table_name (str): Name of the Snowflake target table
            batch_num (int): Batch number for logging
            upsert (bool): If True, update existing records based on DOCUMENT_ID
            existing_doc_ids (set): Set of existing document IDs (used when upsert=True)
        
        Returns:
            tuple: (number of documents inserted, number of documents updated, list of failed document details)
        """
        try:
            if not documents:
                return (0, 0, [])
            
            if existing_doc_ids is None:
                existing_doc_ids = set()
            
            # Optimized batch processing: separate inserts from updates for better performance
            # Batch inserts together (multi-row INSERT) while handling updates individually
            rows_inserted = 0
            rows_updated = 0
            failed_documents = []  # Track failed documents with details
            
            # Prepare documents: validate and separate into inserts/updates
            insert_batch = []  # Documents to insert (will be batched)
            update_docs = []  # Documents to update (handled individually)
            insert_batch_size = 1000  # Number of rows per INSERT statement
            
            collection_name_escaped = collection_name.replace("'", "''")
            
            # First pass: validate and categorize documents
            for doc in documents:
                doc_id = str(doc.get('_id', 'unknown')) if doc.get('_id') else 'unknown'
                json_str = None
                try:
                    # Clean the document to ensure valid JSON
                    cleaned_doc = self._clean_document(doc)
                    
                    # Convert document to JSON string
                    json_str = json.dumps(cleaned_doc, default=str, ensure_ascii=False, separators=(',', ':'))
                    
                    # Validate JSON is valid
                    json.loads(json_str)  # Validate it's valid JSON
                    
                    # Check if document exists and upsert is enabled
                    is_existing = upsert and doc_id and doc_id in existing_doc_ids
                    
                    if is_existing:
                        # Add to update list with document hash for change detection
                        update_docs.append({
                            'doc_id': doc_id,
                            'json_str': json_str,
                            'doc': doc,
                            'doc_hash': get_document_hash(doc)
                        })
                    else:
                        # Add to insert batch with document hash
                        insert_batch.append({
                            'doc_id': doc_id,
                            'json_str': json_str,
                            'doc': doc,
                            'doc_hash': get_document_hash(doc)
                        })
                        
                except Exception as e:
                    # Capture detailed error information
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # Get a sample of the problematic JSON
                    json_sample = None
                    try:
                        if json_str:
                            json_sample = json_str[:200] + "..." if len(json_str) > 200 else json_str
                        else:
                            json_sample = "Unable to extract JSON sample"
                    except:
                        json_sample = "Unable to extract JSON sample"
                    
                    # Store failure details
                    failed_documents.append({
                        'doc_id': doc_id,
                        'error_type': error_type,
                        'error_message': error_msg,
                        'json_sample': json_sample
                    })
                    
                    # Log detailed error
                    if len(failed_documents) <= 10:
                        print(f"   Warning: Failed to prepare document {doc_id}:")
                        print(f"      Error Type: {error_type}")
                        print(f"      Error: {error_msg[:500]}")
                        if json_sample:
                            print(f"      JSON Sample: {json_sample}")
                    elif len(failed_documents) == 11:
                        print(f"   ... (additional errors will be summarized at end)")
            
            # Second pass: Batch insert new documents using UNION ALL for better performance
            if insert_batch:
                # Process inserts in sub-batches for better performance
                for i in range(0, len(insert_batch), insert_batch_size):
                    sub_batch = insert_batch[i:i + insert_batch_size]
                    
                    try:
                        # Build multi-row INSERT using UNION ALL with SELECT
                        # This approach works better with Snowflake's parameter binding
                        select_clauses = []
                        params = []
                        
                        for idx, item in enumerate(sub_batch):
                            # Use indexed parameters to avoid conflicts
                            # Include DOCUMENT_HASH for change detection on future upserts
                            select_clauses.append(f"SELECT PARSE_JSON(%s) AS ROW_DATA, %s AS DOCUMENT_HASH, %s AS COLLECTION_NAME")
                            params.extend([item['json_str'], item.get('doc_hash', ''), collection_name])
                        
                        # Build the INSERT statement with UNION ALL
                        insert_sql = f"""
                        INSERT INTO {table_name} (ROW_DATA, DOCUMENT_HASH, COLLECTION_NAME)
                        {' UNION ALL '.join(select_clauses)}
                        """
                        
                        # Execute batch insert
                        self.snowflake_cursor.execute(insert_sql, params)
                        rows_inserted += len(sub_batch)
                        
                        # Update existing_doc_ids for subsequent processing
                        if upsert:
                            for item in sub_batch:
                                if item['doc_id']:
                                    existing_doc_ids.add(item['doc_id'])
                                    
                    except Exception as e:
                        # If batch insert fails, fall back to individual inserts for this sub-batch
                        error_msg = str(e)
                        if len(error_msg) > 200:
                            error_msg = error_msg[:200] + "..."
                        print(f"   Warning: Batch insert failed, falling back to individual inserts: {error_msg}")
                        for item in sub_batch:
                            try:
                                doc_hash = item.get('doc_hash', '')
                                insert_sql = f"""
                                INSERT INTO {table_name} (ROW_DATA, DOCUMENT_HASH, COLLECTION_NAME)
                                SELECT PARSE_JSON(%s), %s, '{collection_name_escaped}'
                                """
                                self.snowflake_cursor.execute(insert_sql, [item['json_str'], doc_hash])
                                rows_inserted += 1
                                if upsert and item['doc_id']:
                                    existing_doc_ids.add(item['doc_id'])
                            except Exception as e2:
                                error_type = type(e2).__name__
                                error_msg = str(e2)
                                json_sample = item['json_str'][:200] + "..." if len(item['json_str']) > 200 else item['json_str']
                                failed_documents.append({
                                    'doc_id': item['doc_id'],
                                    'error_type': error_type,
                                    'error_message': error_msg,
                                    'json_sample': json_sample
                                })
            
            # Third pass: Update existing documents using BATCHED MERGE
            # Only update if document hash has changed (skip identical documents)
            # Process in chunks to avoid query size limits
            merge_chunk_size = 500  # Documents per MERGE statement
            
            for chunk_start in range(0, len(update_docs), merge_chunk_size):
                chunk = update_docs[chunk_start:chunk_start + merge_chunk_size]
                
                try:
                    # Build VALUES clause for batched MERGE
                    # Pass raw JSON strings, parse in outer SELECT (PARSE_JSON not allowed in VALUES)
                    value_rows = []
                    params = []
                    
                    for item in chunk:
                        value_rows.append("(%s, %s, %s, %s)")
                        params.extend([
                            item['json_str'],
                            collection_name,
                            item['doc_id'],
                            item.get('doc_hash', '')
                        ])
                    
                    merge_sql = f"""
                    MERGE INTO {table_name} AS target
                    USING (
                        SELECT 
                            PARSE_JSON(column1) AS ROW_DATA,
                            column2 AS COLLECTION_NAME,
                            column3 AS mongo_id,
                            column4 AS doc_hash
                        FROM VALUES {', '.join(value_rows)}
                    ) AS source
                    ON target.DOCUMENT_ID = source.mongo_id
                       AND target.COLLECTION_NAME = source.COLLECTION_NAME
                    WHEN MATCHED AND (target.DOCUMENT_HASH IS NULL OR target.DOCUMENT_HASH != source.doc_hash) THEN
                        UPDATE SET 
                            ROW_DATA = source.ROW_DATA,
                            DOCUMENT_HASH = source.doc_hash,
                            LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                    """
                    
                    result = self.snowflake_cursor.execute(merge_sql, params)
                    rows_updated += len(chunk)
                    print(f"   Merged batch of {len(chunk)} documents ({chunk_start + len(chunk)}/{len(update_docs)})")
                    
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    print(f"   Warning: Batch MERGE failed, falling back to individual updates: {error_msg[:200]}")
                    
                    # Fallback to individual MERGE for failed batch
                    for item in chunk:
                        try:
                            doc_id_escaped = item['doc_id'].replace("'", "''")
                            doc_hash = item.get('doc_hash', '')
                            merge_sql = f"""
                            MERGE INTO {table_name} AS target
                            USING (
                                SELECT PARSE_JSON(%s) AS ROW_DATA, 
                                       '{collection_name_escaped}' AS COLLECTION_NAME,
                                       '{doc_id_escaped}' AS mongo_id,
                                       '{doc_hash}' AS doc_hash
                            ) AS source
                            ON target.DOCUMENT_ID = source.mongo_id
                               AND target.COLLECTION_NAME = source.COLLECTION_NAME
                            WHEN MATCHED AND (target.DOCUMENT_HASH IS NULL OR target.DOCUMENT_HASH != source.doc_hash) THEN
                                UPDATE SET 
                                    ROW_DATA = source.ROW_DATA,
                                    DOCUMENT_HASH = source.doc_hash,
                                    LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                            """
                            self.snowflake_cursor.execute(merge_sql, [item['json_str']])
                            rows_updated += 1
                        except Exception as e2:
                            failed_documents.append({
                                'doc_id': item['doc_id'],
                                'error_type': type(e2).__name__,
                                'error_message': str(e2),
                                'json_sample': item['json_str'][:200] if item['json_str'] else None
                            })
            
            # Summary for this batch
            if failed_documents:
                print(f"   Warning: {len(failed_documents)} document(s) failed in this batch (out of {len(documents)} total)")
            
            return rows_inserted, rows_updated, failed_documents
            
        except ProgrammingError as e:
            print(f"Error inserting batch {batch_num}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error inserting batch {batch_num}: {e}")
            raise
    
    def query_table(self, table_name, limit=10, collection_name=None):
        """
        Query and display sample data from the target table.
        
        Args:
            table_name (str): Name of the table
            limit (int): Number of rows to display
            collection_name (str): Optional filter by collection name
        """
        try:
            where_clause = f"WHERE COLLECTION_NAME = '{collection_name}'" if collection_name else ""
            query_sql = f"SELECT ROW_ID, ROW_DATA, COLLECTION_NAME, LOAD_TIMESTAMP FROM {table_name} {where_clause} LIMIT {limit}"
            
            self.snowflake_cursor.execute(query_sql)
            rows = self.snowflake_cursor.fetchall()
            
            print(f"\nSample data from '{table_name}':")
            for row in rows:
                print(f"  Row ID: {row[0]}, Collection: {row[2]}, Timestamp: {row[3]}")
                print(f"    Data: {json.dumps(json.loads(str(row[1])), indent=2)[:200]}...")  # Show first 200 chars
            return rows
        except ProgrammingError as e:
            print(f"Error querying table: {e}")
            raise
    
    def close(self):
        """Close MongoDB and Snowflake connections."""
        # Close connection pool first
        self._close_connection_pool()
        
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB connection closed")
        if self.snowflake_cursor:
            self.snowflake_cursor.close()
        if self.snowflake_conn:
            self.snowflake_conn.close()
            print("Snowflake connection closed")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Export MongoDB Atlas collections to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python mongo_to_snowflake_ingestion.py --dry-run
  python mongo_to_snowflake_ingestion.py --collection merchants --table MERCHANTS
  python mongo_to_snowflake_ingestion.py --workers 8 --batch-size 5000
  python mongo_to_snowflake_ingestion.py --no-parallel --upsert
        """
    )
    
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without inserting data')
    parser.add_argument('--collection', type=str,
                        help='MongoDB collection name to export')
    parser.add_argument('--table', type=str,
                        help='Snowflake target table name')
    parser.add_argument('--schema', type=str,
                        help='Snowflake schema name (overrides SNOWFLAKE_SCHEMA env var)')
    parser.add_argument('--batch-size', type=int, default=25000,
                        help='Number of documents per batch (default: 25000)')
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of parallel worker threads (default: 8)')
    parser.add_argument('--upsert', action='store_true',
                        help='Enable upsert mode (update existing records)')
    parser.add_argument('--no-parallel', action='store_true',
                        help='Disable parallel processing')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of documents to ingest (default: no limit)')
    
    return parser.parse_args()


def main():
    """Example usage of the MongoAtlasSnowflakeExport class."""
    
    # Parse command-line arguments
    args = parse_args()
    
    # MongoDB Atlas connection configuration
    mongo_config = {
        'connection_string': os.getenv('MONGO_CONNECTION_STRING'),
        'database': os.getenv('MONGO_DATABASE')
    }
    
    # Snowflake connection configuration
    # Schema can be overridden with --schema argument
    snowflake_schema = args.schema if args.schema else os.getenv('SNOWFLAKE_SCHEMA', 'EXT')
    
    snowflake_config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': snowflake_schema,
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'private_key_path': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
        'private_key_passphrase': os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
    }
    
    # Debug: Show what credentials are being used (sensitive data masked)
    print("\n=== Connection Configuration ===")
    print(f"MongoDB Database: {mongo_config['database']}")
    print(f"Snowflake Account: {snowflake_config['account']}")
    print(f"Snowflake Database: {snowflake_config['database']}")
    print(f"Snowflake Schema: {snowflake_config['schema']}")
    print("=" * 35 + "\n")
    
    # ============================================================================
    # CONFIGURATION: Update these values for your environment
    # Can be overridden by command-line arguments
    # ============================================================================
   
    collection_name = 'stores'

    table_name = 'STORES'

    query_filter = None
    
    fields_to_extract = ['_id', 'address', 'merchantId', 'storeId', 'storeName']
 
    # Batch size for inserting documents (override with --batch-size)
    batch_size = args.batch_size
    
    # Upsert mode (override with --upsert)
    upsert_mode = args.upsert
    
    # Parallel processing settings (override with --no-parallel, --workers)
    parallel_mode = not args.no_parallel
    num_workers = args.workers
    
    # Dry run mode (override with --dry-run)
    dry_run = args.dry_run
    
    # Document limit (override with --limit)
    doc_limit = None  # No limit - process all documents
    
    # ============================================================================
    
    print("\n" + "=" * 60)
    print("MongoDB Atlas to Snowflake Data Export")
    print("=" * 60)
    print(f"Collection: {collection_name}")
    print(f"Target Table: {table_name}")
    print(f"Batch Size: {batch_size:,}")
    print(f"Upsert Mode: {'Enabled (update existing records)' if upsert_mode else 'Disabled (insert only)'}")
    print(f"Parallel Mode: {'Enabled' if parallel_mode else 'Disabled'}")
    if parallel_mode:
        print(f"Worker Threads: {num_workers}")
    print(f"Dry Run: {'YES - No data will be inserted' if dry_run else 'No'}")
    print(f"Document Limit: {doc_limit:,}" if doc_limit else "Document Limit: No limit")
    if fields_to_extract:
        print(f"Fields to Extract: {', '.join(fields_to_extract)}")
    else:
        print(f"Fields to Extract: All fields")
    print("=" * 60 + "\n")
    
    # Initialize export class
    print("Initializing export class...")
    exporter = MongoAtlasSnowflakeExport(mongo_config, snowflake_config, num_workers=num_workers)
    
    try:
        # Connect to MongoDB and Snowflake
        print("\nStep 1/4: Establishing connections...")
        exporter.connect_mongo()
        exporter.connect_snowflake()
        print("Connections established successfully\n")
        
        # Create target table (if not exists)
        print("Step 2/4: Creating/verifying target table...")
        exporter.create_target_table(table_name)
        print("Table ready\n")
        
        # Export collection to Snowflake
        print("Step 3/4: Exporting collection to Snowflake...")
        rows_inserted = exporter.export_collection_to_snowflake(
            collection_name=collection_name,
            table_name=table_name,
            query=query_filter,
            fields=fields_to_extract,
            batch_size=batch_size,
            upsert=upsert_mode,
            parallel=parallel_mode,
            dry_run=dry_run,
            limit=doc_limit
        )
        print("Export completed\n")
        
        print("=" * 60)
        print("Export Process Completed Successfully!")
        print(f"   Total documents inserted: {rows_inserted}")
        print("=" * 60 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("Error during export process")
        print("=" * 60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 60 + "\n")
    finally:
        print("Step 4/4: Closing connections...")
        exporter.close()
        print("All connections closed\n")


if __name__ == '__main__':
    main()
