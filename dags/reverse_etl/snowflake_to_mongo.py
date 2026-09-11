#!/usr/bin/env python3
"""
Snowflake to MongoDB Reverse ETL Script

Performs UPDATE operations from Snowflake to MongoDB's transactions collection.
- Only updates existing records (matched by transaction_id)
- Does NOT insert new records
- Uses parallel processing for high throughput

"""
import os
import sys
import argparse
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import threading


# =============================================================================
# CONFIGURATION PRESETS
# =============================================================================
# Define presets for common reverse ETL scenarios
# Each preset specifies: table, collection, match_field, and field_mapping

PRESETS = {
    "transactions": {
        "description": "Update transactions",
        "view": "TRANSACTIONS_VW",
        "database": "ANALYTICS_PROD",  # Snowflake database (overrides connection database if different)
        "schema": "[SCHEMA]",
        "collection": "transactions",
        "match_field": "TRANSACTION_ID",  # Snowflake column used to match MongoDB docs
        "mongo_match_field": "transactionId",  # MongoDB field to match against
        "field_mapping": {
            "AMOUNT": "Amount",
        }
    }
}

# Default preset to use
DEFAULT_PRESET = "transactions"


# =============================================================================
# DEFAULT CONFIGURATION (can be overridden via args or environment)
# =============================================================================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "[SNOWFLAKE-ACCOUNT]")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "[SF-DBT-SERVICE-USER]")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ELT_TRANSFORM_PROD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ELT_WH_PROD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_PROD")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "[TARGET-SCHEMA]")

# RSA Key Authentication
# Default to Airflow path, but allow override via environment variable
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv(
    "SNOWFLAKE_PRIVATE_KEY_PATH", 
    "/opt/airflow/keys/DBT-PROD-RSA-KEY.P8"  # Airflow path (default)
)
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "[DBT-SECRET-KEY]")

# Legacy defaults (for backward compatibility)
MONGODB_URI = os.getenv("MONGODB_URI", MONGODB_URI_DEV)
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", MONGODB_DATABASE_DEV)


FIELD_MAPPING = { 
    "AMOUNT": "Amount",
}

# Default match fields
MATCH_FIELD = "TRANSACTION_ID"  # Snowflake column
MONGO_MATCH_FIELD = "transactionId"  # MongoDB field


def load_private_key(key_path: str, passphrase: str):
    """Load RSA private key from file."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    
    key_path = actual_path
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
    
    # Convert to DER format for Snowflake
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return pkb


def load_private_key_from_content(key_content: str, passphrase: str):
    """Load RSA private key from base64-encoded content string."""
    import base64
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    try:
        # Try base64 decode first (SSM stores keys base64-encoded)
        key_data = base64.b64decode(key_content)
    except Exception:
        # If not base64, use as-is (raw PEM)
        key_data = key_content.encode('utf-8') if isinstance(key_content, str) else key_content

    p_key = serialization.load_pem_private_key(
        key_data,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return pkb


def get_snowflake_connection():
    """Create and return a Snowflake connection using RSA key authentication.
    
    Reads from environment variables at RUNTIME (not import time) to allow
    DAGs to set these values before calling this function.
    
    Supports both file-based key (SNOWFLAKE_PRIVATE_KEY_PATH) and
    inline base64-encoded key (SNOWFLAKE_PRIVATE_KEY).
    """
    import snowflake.connector

    # Read all config from environment at runtime (allows DAG to override)
    account = os.getenv("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT)
    user = os.getenv("SNOWFLAKE_USER", SNOWFLAKE_USER)
    role = os.getenv("SNOWFLAKE_ROLE", SNOWFLAKE_ROLE)
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE)
    database = os.getenv("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE)
    schema = os.getenv("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA)
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", SNOWFLAKE_PRIVATE_KEY_PATH)
    key_content = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
    
    # Debug: Show which database we're connecting to
    print(f"  Connecting to: {database}.{schema}")
    
    # Load the private key - prefer inline content over file path
    if key_content:
        print(f"  Using inline private key from SNOWFLAKE_PRIVATE_KEY env var")
        private_key = load_private_key_from_content(key_content, key_passphrase)
    else:
        private_key = load_private_key(key_path, key_passphrase)
    
    return snowflake.connector.connect(
        account=account,
        user=user,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        private_key=private_key,
    )


def get_mongo_config(target: Optional[str] = None):
    """
    Get MongoDB configuration based on target environment.
    
    Args:
        target: Target environment ('dev' or 'prod'). If None, uses environment variables or defaults to dev.
    
    Returns:
        dict: MongoDB connection configuration with 'connection_string' and 'database'
    """
    # Default to dev if not provided
    if target is None:
        target = 'dev'
    target = str(target).lower()
    
    # Default values for dev and prod
    dev_defaults = {
        'connection_string': MONGODB_URI_DEV,
        'database': MONGODB_DATABASE_DEV,
    }
    
    prod_defaults = {
        'connection_string': MONGODB_URI_PROD,
        'database': MONGODB_DATABASE_PROD,
    }
    
    if target == 'prod':
        # Check for prod-specific environment variables first, then use defaults
        return {
            'connection_string': os.getenv('MONGO_CONNECTION_STRING_PROD', 
                os.getenv('MONGODB_URI', prod_defaults['connection_string'])),
            'database': os.getenv('MONGODB_DATABASE_PROD', 
                os.getenv('MONGODB_DATABASE', prod_defaults['database'])),
            'target': 'prod',
        }
    else:
        # Check for dev-specific environment variables first, then use defaults
        return {
            'connection_string': os.getenv('MONGO_CONNECTION_STRING_DEV', 
                os.getenv('MONGODB_URI', dev_defaults['connection_string'])),
            'database': os.getenv('MONGODB_DATABASE_DEV', 
                os.getenv('MONGODB_DATABASE', dev_defaults['database'])),
            'target': 'dev',
        }


def get_mongodb_connection(collection_name: str = "transactions"):
    """Create and return MongoDB client and collection.
    
    Reads from environment variables at RUNTIME to allow DAGs to override.
    """
    from pymongo import MongoClient

    uri = os.getenv("MONGODB_URI", MONGODB_URI)
    database = os.getenv("MONGODB_DATABASE", MONGODB_DATABASE)
    client = MongoClient(uri)
    db = client[database]
    collection = db[collection_name]
    return client, collection


def get_snowflake_count(
    cursor,
    table_name: str,
    where_clause: Optional[str] = None
) -> int:
    """Get total count of records to process."""
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    if where_clause:
        count_query += f" WHERE {where_clause}"
    
    cursor.execute(count_query)
    return cursor.fetchone()[0]


def fetch_snowflake_batches(
    cursor,
    table_name: str,
    batch_size: int = 10000,
    where_clause: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    match_field: str = "TRANSACTION_ID"
):
    """
    Generator that yields batches of records from Snowflake.
    
    Streams data in batches to minimize memory usage.
    Only selects match_field (for matching) plus fields in field_mapping.
    
    Args:
        cursor: Snowflake cursor
        table_name: Source table name
        batch_size: Records per batch
        where_clause: Optional SQL WHERE clause
        field_mapping: Dict of Snowflake column -> MongoDB field mappings
        match_field: Snowflake column used for matching (e.g., TRANSACTION_ID)
    
    Yields:
        List[Dict[str, Any]]: Batch of records
    """
    # Use provided field_mapping or fall back to global default
    mapping = field_mapping or FIELD_MAPPING
    
    # Build column list: match_field (for matching) + mapped fields
    mapped_columns = list(mapping.keys())
    all_columns = [match_field] + mapped_columns
    columns = ", ".join(all_columns)
    
    # Data query
    query = f"SELECT {columns} FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    
    cursor.execute(query)
    
    # Get column names from cursor description
    column_names = [desc[0] for desc in cursor.description]
    
    batch = []
    for row in cursor:
        record = dict(zip(column_names, row))
        batch.append(record)
        
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Yield remaining records
    if batch:
        yield batch


def transform_record(
    snowflake_record: Dict[str, Any],
    field_mapping: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Transform a Snowflake record to MongoDB document format.
    
    Applies field mapping and any necessary type conversions.
    Skips fields that:
    - Don't exist in source record
    - Have null values or are blank strings
    
    Args:
        snowflake_record: Source record from Snowflake
        field_mapping: Dict of Snowflake column -> MongoDB field mappings
    
    Returns:
        Dict with MongoDB field names and transformed values
    """
    # Use provided field_mapping or fall back to global default
    mapping = field_mapping or FIELD_MAPPING
    
    mongo_doc = {}
    
    for sf_field, mongo_field in mapping.items():
        # Skip if field doesn't exist in source record
        if sf_field not in snowflake_record:
            continue
            
        value = snowflake_record[sf_field]
        
        # Skip null/None values - don't add field if source is null
        if value is None:
            continue
        
        # Skip empty or whitespace-only strings
        if isinstance(value, str) and not value.strip():
            continue
        
        # Ensure fees field is always stored as an array of objects
        if mongo_field == "fees":
            if value is None:
                value = []
            elif isinstance(value, str):
                # Parse JSON string to array of objects
                try:
                    value = json.loads(value)
                    # Ensure it's a list
                    if not isinstance(value, list):
                        value = [value] if value else []
                except (json.JSONDecodeError, ValueError):
                    # If JSON parsing fails, default to empty array
                    value = []
            elif not isinstance(value, list):
                # If it's not a list, wrap it in a list
                value = [value] if value else []
            
            # Ensure feeAmount is stored as DOUBLE (float) in each fee object
            if isinstance(value, list):
                for fee_obj in value:
                    if isinstance(fee_obj, dict) and "feeAmount" in fee_obj:
                        try:
                            fee_obj["feeAmount"] = float(fee_obj["feeAmount"])
                        except (ValueError, TypeError):
                            # If conversion fails, set to 0.0 or keep original
                            fee_obj["feeAmount"] = 0.0
        
        # Convert Snowflake datetime to Python datetime if needed
        if isinstance(value, datetime):
            # Ensure timezone awareness
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
        
        mongo_doc[mongo_field] = value
    
    return mongo_doc


def update_mongodb(
    collection,
    records: List[Dict[str, Any]],
    batch_size: int = 1000,
    field_mapping: Optional[Dict[str, str]] = None,
    match_field: str = "TRANSACTION_ID",
    mongo_match_field: str = "transactionId"
) -> Dict[str, int]:
    """
    Perform UPDATE operations on MongoDB collection (no inserts) using bulk operations.
    
    Only updates existing documents that match by the specified match field.
    Uses bulk_write for much better performance than individual operations.
    
    Args:
        collection: MongoDB collection
        records: List of Snowflake records to process
        batch_size: Batch size for bulk operations
        field_mapping: Dict of Snowflake column -> MongoDB field mappings
        match_field: Snowflake column used for matching (e.g., TRANSACTION_ID)
        mongo_match_field: MongoDB field to match against (e.g., transactionId)
    
    Returns:
        dict: Statistics about the operation (matched, modified, errors)
    """
    from pymongo import UpdateOne

    # Use provided field_mapping or fall back to global default
    mapping = field_mapping or FIELD_MAPPING
    
    stats = {
        "matched": 0,
        "modified": 0,
        "errors": 0,
    }
    
    if not records:
        return stats
    
    # Create UTC datetime with full timestamp (including seconds and microseconds)
    current_time = datetime.now(timezone.utc)
    
    # Build bulk operations list
    operations = []
    
    for record in records:
        # Step 1: Get match value from Snowflake record
        match_value = record.get(match_field)
        
        # Skip if match_value is NULL, None, empty, or whitespace
        if match_value is None:
            stats["skipped_null"] = stats.get("skipped_null", 0) + 1
            continue
        if isinstance(match_value, str) and not match_value.strip():
            stats["skipped_empty"] = stats.get("skipped_empty", 0) + 1
            continue
        
        # Step 2: Transform source fields
        source_fields = transform_record(record, field_mapping=mapping)
        
        if not source_fields:
            stats["no_source_fields"] = stats.get("no_source_fields", 0) + 1
            continue
        
        # Step 3: Build update - use mapped field names directly
        fields_to_update = {}
        
        for field_name, new_value in source_fields.items():
            # Use the mapped field name directly
            fields_to_update[field_name] = new_value
        
        if not fields_to_update:
            stats["no_source_fields"] = stats.get("no_source_fields", 0) + 1
            continue
        
        # Add recon_ops_updated_at timestamp and version flag
        # Stored as UTC datetime object with full timestamp (including seconds and microseconds)
        fields_to_update["recon_ops_updated_at"] = current_time
        fields_to_update["recon_ops_version"] = True
        
        # Step 4: Create UpdateOne operation (upsert=False means only update existing docs)
        operations.append(
            UpdateOne(
                {mongo_match_field: match_value},
                {"$set": fields_to_update},
                upsert=False  # Only update existing documents, don't insert
            )
        )
    
    # Step 5: Execute bulk operations in batches
    if not operations:
        return stats
    
    # Process in batches to avoid memory issues
    for i in range(0, len(operations), batch_size):
        batch_ops = operations[i:i + batch_size]
        try:
            batch_result = _execute_bulk_write(collection, batch_ops)
            stats["matched"] += batch_result["matched"]
            stats["modified"] += batch_result["modified"]
            stats["errors"] += batch_result["errors"]
        except Exception as e:
            stats["errors"] += len(batch_ops)
            print(f"  Error in bulk write batch: {e}")
    
    # Calculate not_found count (operations that didn't match)
    stats["not_found"] = len(operations) - stats["matched"] - stats["errors"]
    
    return stats


def _execute_bulk_write(collection, operations) -> Dict[str, int]:
    """Execute bulk write operations and return statistics."""
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError

    result = {
        "matched": 0,
        "modified": 0,
        "errors": 0,
    }
    
    try:
        bulk_result = collection.bulk_write(operations, ordered=False)
        result["matched"] = bulk_result.matched_count
        result["modified"] = bulk_result.modified_count
    except BulkWriteError as e:
        # Partial success - extract stats
        details = e.details
        result["matched"] = details.get("nMatched", 0)
        result["modified"] = details.get("nModified", 0)
        result["errors"] = len(details.get("writeErrors", []))
    except Exception as e:
        result["errors"] = len(operations)
    
    return result


# Thread-local storage for MongoDB connections
_thread_local = threading.local()

# Runtime configuration (set by run_update before spawning workers)
_runtime_config = {
    "field_mapping": FIELD_MAPPING,
    "match_field": MATCH_FIELD,
    "mongo_match_field": MONGO_MATCH_FIELD,
}


def get_thread_mongodb_collection(collection_name: Optional[str] = None):
    """Get a MongoDB collection for the current thread (thread-safe connection pooling)."""
    from pymongo import MongoClient

    coll_name = collection_name or _runtime_config.get("collection_name")
    if not coll_name:
        raise ValueError("collection_name not set in runtime config")
    
    # Read from environment variables at runtime (allows DAGs to override)
    mongo_uri = os.getenv("MONGODB_URI", MONGODB_URI)
    mongo_database = os.getenv("MONGODB_DATABASE", MONGODB_DATABASE)
    
    if not hasattr(_thread_local, "mongo_client"):
        _thread_local.mongo_client = MongoClient(mongo_uri)
    
    # Check if we need to switch collections
    if not hasattr(_thread_local, "collection_name") or _thread_local.collection_name != coll_name:
        _thread_local.collection = _thread_local.mongo_client[mongo_database][coll_name]
        _thread_local.collection_name = coll_name
    
    return _thread_local.collection


def process_batch_worker(batch: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Worker function to process a batch of records.
    Each worker uses its own MongoDB connection from the thread pool.
    Uses runtime config for field mapping and match fields.
    """
    collection = get_thread_mongodb_collection(_runtime_config.get("collection_name"))
    return update_mongodb(
        collection, 
        batch, 
        batch_size=len(batch),
        field_mapping=_runtime_config.get("field_mapping"),
        match_field=_runtime_config.get("match_field", MATCH_FIELD),
        mongo_match_field=_runtime_config.get("mongo_match_field", MONGO_MATCH_FIELD)
    )


def run_update(
    batch_size: int = 1000,
    where_clause: Optional[str] = None,
    dry_run: bool = False,
    parallel_workers: int = 4,
    table_name: Optional[str] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    collection_name: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    match_field: Optional[str] = None,
    mongo_match_field: Optional[str] = None
) -> Dict[str, Any]:

    global _runtime_config
    
    # Resolve configuration (use provided values or fall back to defaults)
    if not table_name:
        raise ValueError("table_name is required - use --table or --preset")
    if not collection_name:
        raise ValueError("collection_name is required - use --collection or --preset")
    
    source_table = table_name
    source_database = database_name or os.getenv("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE)
    source_schema = schema_name or SNOWFLAKE_SCHEMA
    target_collection = collection_name
    mapping = field_mapping or FIELD_MAPPING
    sf_match_field = match_field or MATCH_FIELD
    mongo_match = mongo_match_field or MONGO_MATCH_FIELD
    
    # If preset specifies a database/schema, set environment variables before connecting
    # This ensures we connect to the correct database from the start
    if database_name:
        os.environ["SNOWFLAKE_DATABASE"] = database_name
    if schema_name:
        os.environ["SNOWFLAKE_SCHEMA"] = schema_name
    
    # Set runtime config for worker threads
    _runtime_config = {
        "collection_name": target_collection,
        "field_mapping": mapping,
        "match_field": sf_match_field,
        "mongo_match_field": mongo_match,
    }
    
    stats = {
        "snowflake_records": 0,
        "matched": 0,
        "modified": 0,
        "errors": 0,
        "batches_processed": 0,
        "start_time": datetime.now(),
        "end_time": None,
    }
    
    # Connect to Snowflake (will use the database/schema from preset if set above)
    print("Connecting to Snowflake...")
    try:
        sf_conn = get_snowflake_connection()
        sf_cursor = sf_conn.cursor()
        database = source_database
        print(f"✓ Connected to Snowflake: {database}.{source_schema}")
    except Exception as e:
        print(f"ERROR: Could not connect to Snowflake: {e}", file=sys.stderr)
        raise
    
    # Test MongoDB connection
    print("Connecting to MongoDB...")
    try:
        from pymongo import MongoClient

        mongo_uri = os.getenv("MONGODB_URI", MONGODB_URI)
        mongo_database = os.getenv("MONGODB_DATABASE", MONGODB_DATABASE)
        mongo_client = MongoClient(mongo_uri)
        mongo_collection = mongo_client[mongo_database][target_collection]
        mongo_client.admin.command("ping")
        print(f"✓ Connected to MongoDB: {mongo_database}.{target_collection}")
        mongo_client.close()  # Close test connection, workers will create their own
    except Exception as e:
        print(f"ERROR: Could not connect to MongoDB: {e}", file=sys.stderr)
        sf_conn.close()
        raise
    
    try:
        # Get total count for progress bar
        print(f"\nFetching data from {source_table}...")
        if where_clause:
            print(f"  Filter: {where_clause}")
        
        total_count = get_snowflake_count(sf_cursor, source_table, where_clause)
        print(f"  Found {total_count:,} records to process")
        
        if total_count == 0:
            print("No records to process.")
            stats["end_time"] = datetime.now()
            return stats
        
        if dry_run:
            print("\n=== DRY RUN MODE ===")
            print(f"Would process {total_count:,} records")
            print(f"Source table: {source_table}")
            print(f"Target collection: {target_collection}")
            print(f"Match field: {sf_match_field} -> {mongo_match}")
            print(f"Fields to update: {list(mapping.keys())}")
            print(f"Batch size: {batch_size:,}")
            print(f"Parallel workers: {parallel_workers}")
            print(f"Estimated batches: {(total_count + batch_size - 1) // batch_size}")
            
            # Fetch one batch to show sample
            for batch in fetch_snowflake_batches(
                sf_cursor, source_table, batch_size=1, 
                where_clause=where_clause, field_mapping=mapping, match_field=sf_match_field
            ):
                if batch:
                    print("\nSample record (first):")
                    print(f"  {sf_match_field}: {batch[0].get(sf_match_field)}")
                    sample = transform_record(batch[0], field_mapping=mapping)
                    for key, value in sample.items():
                        print(f"  {key}: {value}")
                break
            
            stats["snowflake_records"] = total_count
            stats["end_time"] = datetime.now()
            return stats
        
        # Process in parallel with streaming batches
        print(f"\nUpdating MongoDB with {total_count:,} records...")
        print(f"  Batch size: {batch_size:,} | Workers: {parallel_workers}")
        
        try:
            from tqdm import tqdm
        except ImportError:
            class _tqdm_stub:
                """Fallback no-op progress bar when tqdm is not installed."""
                def __init__(self, *args: object, **kwargs: object) -> None: pass
                def __enter__(self) -> "_tqdm_stub": return self
                def __exit__(self, *args: object) -> None: pass
                def update(self, n: int = 1) -> None: pass
            tqdm: type = _tqdm_stub  # type: ignore[no-redef]

        with tqdm(total=total_count, desc="  Updating", unit="rec") as pbar:
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = []
                
                # Stream batches from Snowflake and submit to thread pool
                for batch in fetch_snowflake_batches(
                    sf_cursor, 
                    source_table, 
                    batch_size=batch_size, 
                    where_clause=where_clause,
                    field_mapping=mapping,
                    match_field=sf_match_field
                ):
                    stats["snowflake_records"] += len(batch)
                    future = executor.submit(process_batch_worker, batch)
                    futures.append((future, len(batch)))
                    
                    # Limit pending futures to prevent memory issues
                    if len(futures) >= parallel_workers * 2:
                        # Wait for oldest futures to complete
                        completed_futures = []
                        for f, batch_len in futures:
                            if f.done():
                                result = f.result()
                                stats["matched"] += result["matched"]
                                stats["modified"] += result["modified"]
                                stats["errors"] += result["errors"]
                                stats["batches_processed"] += 1
                                pbar.update(batch_len)
                                completed_futures.append((f, batch_len))
                        
                        # Remove completed futures
                        for item in completed_futures:
                            futures.remove(item)
                
                # Wait for remaining futures
                for future, batch_len in futures:
                    result = future.result()
                    stats["matched"] += result["matched"]
                    stats["modified"] += result["modified"]
                    stats["errors"] += result["errors"]
                    stats["batches_processed"] += 1
                    pbar.update(batch_len)
        
    finally:
        # Close Snowflake connection
        sf_cursor.close()
        sf_conn.close()
    
    stats["end_time"] = datetime.now()
    return stats


def list_presets():
    """Print available presets and exit."""
    print("\n" + "="*60)
    print("AVAILABLE PRESETS")
    print("="*60)
    for name, config in PRESETS.items():
        print(f"\n{name}:")
        print(f"  Description: {config.get('description', 'No description')}")
        database = config.get('database', 'default from connection')
        schema = config.get('schema', SNOWFLAKE_SCHEMA)
        view = config.get('view', config.get('table', 'N/A'))
        print(f"  Table: {database}.{schema}.{view}")
        print(f"  Collection: {config['collection']}")
        print(f"  Match: {config['match_field']} -> {config['mongo_match_field']}")
        print(f"  Fields: {', '.join(config['field_mapping'].keys())}")
    print("\n" + "="*60)


def parse_field_mapping(fields_str: str) -> Dict[str, str]:
    """
    Parse a comma-separated field mapping string.
    
    Format: SF_FIELD1:mongo_field1,SF_FIELD2:mongo_field2
    
    Example: VALUE_DATE:valueDate,AMOUNT:amount
    """
    mapping = {}
    for pair in fields_str.split(","):
        pair = pair.strip()
        if ":" in pair:
            sf_field, mongo_field = pair.split(":", 1)
            mapping[sf_field.strip().upper()] = mongo_field.strip()
        else:
            # If no colon, use same name (lowercase for MongoDB)
            mapping[pair.strip().upper()] = pair.strip().lower()
    return mapping


def main():
    parser = argparse.ArgumentParser(
        description="Reverse ETL: Update MongoDB documents from Snowflake (UPDATE only, no inserts)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Target/environment options
    parser.add_argument(
        "--target",
        type=str,
        choices=["dev", "prod"],
        default="dev",
        help="MongoDB target environment: 'dev' (db_1_dev) or 'prod' (db_1). Default: dev"
    )
    
    # Preset/configuration options
    parser.add_argument(
        "--preset",
        type=str,
        required=True,
        choices=list(PRESETS.keys()),
        help=f"Use a predefined configuration preset (required, available: {', '.join(PRESETS.keys())})"
    )
    
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List all available presets and exit"
    )
    
    # Field mapping options
    parser.add_argument(
        "--fields", "-f",
        type=str,
        help="Field mapping as comma-separated pairs: SF_FIELD:mongo_field,... (overrides preset fields)"
    )
    
    # Processing options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for bulk operations (default: 5000)"
    )
    
    parser.add_argument(
        "--parallel", "-p",
        type=int,
        default=4,
        help="Number of parallel workers for MongoDB writes (default: 4)"
    )
    
    parser.add_argument(
        "--where",
        type=str,
        help="SQL WHERE clause to filter Snowflake records (e.g., \"TRANSACTION_DATE >= '2024-01-01'\")"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually updating"
    )
    
    args = parser.parse_args()
    
    # Handle --list-presets
    if args.list_presets:
        list_presets()
        sys.exit(0)
    
    # Set MongoDB configuration based on target
    mongo_config = get_mongo_config(target=args.target)
    os.environ["MONGODB_URI"] = mongo_config["connection_string"]
    os.environ["MONGODB_DATABASE"] = mongo_config["database"]
    
    print(f"\nMongoDB Target: {mongo_config['target'].upper()}")
    print(f"MongoDB Database: {mongo_config['database']}")
    
    # Resolve configuration from preset
    preset = PRESETS[args.preset]
    table_name = preset["view"]  # Use "view" from preset
    database_name = preset.get("database")  # Database from preset (if specified)
    schema_name = preset.get("schema", SNOWFLAKE_SCHEMA)
    collection_name = preset["collection"]
    match_field = preset["match_field"]
    mongo_match_field = preset["mongo_match_field"]
    
    # Field mapping: use --fields if provided, otherwise use preset
    if args.fields:
        field_mapping = parse_field_mapping(args.fields)
    else:
        field_mapping = preset["field_mapping"]
    
    print(f"\nUsing preset: {args.preset}")
    
    # Validate Snowflake configuration
    missing_config = []
    if not SNOWFLAKE_ACCOUNT:
        missing_config.append("SNOWFLAKE_ACCOUNT")
    if not SNOWFLAKE_USER:
        missing_config.append("SNOWFLAKE_USER")
    if not SNOWFLAKE_WAREHOUSE:
        missing_config.append("SNOWFLAKE_WAREHOUSE")
    if not SNOWFLAKE_DATABASE:
        missing_config.append("SNOWFLAKE_DATABASE")
    if not schema_name:
        missing_config.append("SNOWFLAKE_SCHEMA")
    if not SNOWFLAKE_PRIVATE_KEY_PATH:
        missing_config.append("SNOWFLAKE_PRIVATE_KEY_PATH")
    
    if missing_config:
        print(f"ERROR: Missing required configuration: {', '.join(missing_config)}", file=sys.stderr)
        print("\nPlease set the following environment variables:")
        for var in missing_config:
            print(f"  export {var}='your_value'")
        sys.exit(1)
    
    # Validate private key file exists
    if not os.path.exists(SNOWFLAKE_PRIVATE_KEY_PATH):
        print(f"ERROR: Private key file not found: {SNOWFLAKE_PRIVATE_KEY_PATH}", file=sys.stderr)
        sys.exit(1)
    
    # Validate field mapping (should always exist from preset, but check for safety)
    if not field_mapping:
        print("ERROR: No fields specified in preset", file=sys.stderr)
        sys.exit(1)
    
    # Get current MongoDB config (may have been overridden by environment variables)
    current_mongo_db = os.getenv("MONGODB_DATABASE", mongo_config["database"])
    current_target = mongo_config["target"]
    
    # Determine source database (use preset database if specified, otherwise env var)
    source_database = database_name or SNOWFLAKE_DATABASE
    
    print("="*60)
    print("SNOWFLAKE TO MONGODB REVERSE ETL (UPDATE ONLY)")
    print("="*60)
    print(f"\nSource: {source_database}.{schema_name}.{table_name}")
    print(f"MongoDB Target: {current_target.upper()}")
    print(f"Target: {current_mongo_db}.{collection_name}")
    print(f"Match: {match_field} -> {mongo_match_field}")
    print(f"Fields: {', '.join(f'{k}->{v}' for k, v in field_mapping.items())}")
    print(f"Started at: {datetime.now()}")
    
    try:
        stats = run_update(
            batch_size=args.batch_size,
            where_clause=args.where,
            dry_run=args.dry_run,
            parallel_workers=args.parallel,
            table_name=table_name,
            database_name=database_name,
            schema_name=schema_name,
            collection_name=collection_name,
            field_mapping=field_mapping,
            match_field=match_field,
            mongo_match_field=mongo_match_field
        )
        
        # Print summary
        print("\n" + "="*60)
        print("UPDATE SUMMARY")
        print("="*60)
        print(f"Snowflake records processed: {stats['snowflake_records']:,}")
        print(f"Batches processed:           {stats.get('batches_processed', 0):,}")
        print(f"MongoDB documents matched:   {stats['matched']:,}")
        print(f"MongoDB documents modified:  {stats['modified']:,}")
        print(f"Errors:                      {stats['errors']:,}")
        
        # Debug stats
        if stats.get('skipped_null', 0) > 0:
            print(f"Skipped (NULL transaction_id): {stats['skipped_null']:,}")
        if stats.get('skipped_empty', 0) > 0:
            print(f"Skipped (empty transaction_id): {stats['skipped_empty']:,}")
        if stats.get('not_found', 0) > 0:
            print(f"Not found in MongoDB:        {stats['not_found']:,}")
        if stats.get('no_source_fields', 0) > 0:
            print(f"No source fields to update:  {stats['no_source_fields']:,}")
        if stats.get('no_matching_fields', 0) > 0:
            print(f"No matching fields in schema: {stats['no_matching_fields']:,}")
        if stats.get('unchanged', 0) > 0:
            print(f"Unchanged (same values):     {stats['unchanged']:,}")
        
        if stats["start_time"] and stats["end_time"]:
            duration = stats["end_time"] - stats["start_time"]
            print(f"\nDuration: {duration}")
        
        print(f"Completed at: {datetime.now()}")
        
        if stats["errors"] > 0:
            sys.exit(1)
        
    except Exception as e:
        print(f"\nFatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
        sys.exit(130)
