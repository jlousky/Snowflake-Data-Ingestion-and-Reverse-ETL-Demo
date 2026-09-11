"""
Python script to ingest CSV data from S3 into Snowflake using stage mechanism.
Each CSV row is converted to JSON and stored as VARIANT data type.
"""

import os
import re
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load environment variables from .env file
load_dotenv()


def extract_date_from_filename(filename):
    """
    Extract date from filename like 'report_2025_01_15.csv'.
    
    Args:
        filename (str): The filename to extract date from
        
    Returns:
        datetime: The extracted date, or None if no date found
    """
    # Match pattern: YYYY_MM_DD at the end of filename before .csv
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})\.csv$', filename, re.IGNORECASE)
    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    return None


def extract_merchant_from_path(filepath):
    """
    Extract merchant name from file path.
    
    Args:
        filepath (str): The file path
        
    Returns:
        str: The merchant name, or 'unknown' if not found
    """
    parts = filepath.split('/')
    if len(parts) >= 2:
        return parts[1]
    return 'unknown'


def filter_latest_per_merchant(files_info):
    """
    For each merchant, keep only the file with the most recent date.
    
    Args:
        files_info: List of tuples (rel_path, full_path, content_hash)
        
    Returns:
        List of tuples with only the latest file per merchant
    """
    from collections import defaultdict
    
    merchant_files = defaultdict(list)
    
    # Group files by merchant
    for rel_path, full_path, content_hash in files_info:
        merchant = extract_merchant_from_path(rel_path)
        file_date = extract_date_from_filename(rel_path)
        merchant_files[merchant].append((rel_path, full_path, content_hash, file_date))
    
    # For each merchant, keep only the latest file
    latest_files = []
    for merchant, files in merchant_files.items():
        # Filter to files with valid dates
        files_with_dates = [f for f in files if f[3] is not None]
        
        if files_with_dates:
            # Sort by date descending, take the first (latest)
            files_with_dates.sort(key=lambda x: x[3], reverse=True)
            latest = files_with_dates[0]
            latest_files.append((latest[0], latest[1], latest[2]))  # (rel_path, full_path, content_hash)
            print(f"  Merchant '{merchant}': latest file is {latest[0]} (date: {latest[3].strftime('%Y-%m-%d')})")
        else:
            # No files with dates for this merchant, include all
            for f in files:
                latest_files.append((f[0], f[1], f[2]))
            print(f"  Merchant '{merchant}': no dates found, including all {len(files)} file(s)")
    
    return latest_files


class SnowflakeS3Ingestion:
    """Class to handle S3 to Snowflake data ingestion using existing stages.
    
    Note: This class assumes the external stage already exists in Snowflake.
    The stage should be created separately before using this class.
    """
    
    def __init__(self, snowflake_config):
        """
        Initialize Snowflake connection.
        
        Args:
            snowflake_config (dict): Dictionary containing Snowflake connection parameters:
                - account: Snowflake account identifier
                - user: Snowflake username
                - password: Snowflake password (or use authenticator)
                - warehouse: Snowflake warehouse name
                - database: Snowflake database name
                - schema: Snowflake schema name
                - role: Snowflake role (optional)
        """
        self.config = snowflake_config
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to Snowflake."""
        try:
            # Build connection parameters
            conn_params = {
                'account': self.config['account'],
                'user': self.config['user'],
                'warehouse': self.config['warehouse'],
                'database': self.config['database'],
                'schema': self.config['schema'],
                'role': self.config.get('role'),
            }
            
            # Use private key authentication if private_key_path or private_key content is provided
            private_key_path = self.config.get('private_key_path')
            private_key_content = self.config.get('private_key')  # base64-encoded PEM key
            private_key_passphrase = self.config.get('private_key_passphrase', '')
            
            if private_key_path or private_key_content:
                if private_key_path:
                    print(f"Using private key authentication from file: {private_key_path}")
                    with open(private_key_path, 'rb') as key_file:
                        key_data = key_file.read()
                elif private_key_content:
                    print(f"Using private key authentication from inline content")
                    try:
                        # Try base64 decode first (SSM stores keys base64-encoded)
                        key_data = base64.b64decode(private_key_content)
                    except Exception:
                        # If not base64, use as-is (raw PEM)
                        key_data = private_key_content.encode('utf-8') if isinstance(private_key_content, str) else private_key_content
                
                p_key = serialization.load_pem_private_key(
                    key_data,
                    password=private_key_passphrase.encode() if private_key_passphrase else None,
                    backend=default_backend()
                )
                
                # Get the private key bytes in DER format
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                conn_params['private_key'] = pkb
            else:
                # Fall back to password authentication
                conn_params['password'] = self.config.get('password')
                conn_params['authenticator'] = self.config.get('authenticator', 'snowflake')
            
            self.conn = connect(**conn_params)
            self.cursor = self.conn.cursor()
            
            # Explicitly set database and schema to ensure session context is set
            # Note: Snowflake identifiers are case-insensitive but stored in uppercase
            # Try without quotes first (standard), then with quotes if needed
            if self.config.get('database'):
                db_name = self.config['database']
                try:
                    # Try without quotes first (standard Snowflake behavior)
                    self.cursor.execute(f'USE DATABASE {db_name}')
                    print(f"Set database context to: {db_name}")
                except ProgrammingError as e:
                    # If that fails, try with quotes (for case-sensitive names)
                    try:
                        self.cursor.execute(f'USE DATABASE "{db_name}"')
                        print(f"Set database context to: {db_name} (quoted)")
                    except ProgrammingError as e2:
                        print(f"Error: Could not set database context: {e2}")
                        print(f"Database '{db_name}' may not exist or you may not have access")
                        print(f"Please verify the database name and your permissions")
                        raise
            
            if self.config.get('schema'):
                schema_name = self.config['schema']
                try:
                    # Try without quotes first (standard Snowflake behavior)
                    self.cursor.execute(f'USE SCHEMA {schema_name}')
                    print(f"Set schema context to: {schema_name}")
                except ProgrammingError as e:
                    # If that fails, try with quotes (for case-sensitive names)
                    try:
                        self.cursor.execute(f'USE SCHEMA "{schema_name}"')
                        print(f"Set schema context to: {schema_name} (quoted)")
                    except ProgrammingError as e2:
                        print(f"Warning: Could not set schema context: {e2}")
                        print(f"Schema '{schema_name}' may not exist or you may not have access")
                        print(f"Will use fully qualified names for stage references")
                        # Don't raise - we can still work with fully qualified names
            
            print(f"Successfully connected to Snowflake (Database: {self.config.get('database')}, Schema: {self.config.get('schema')})")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise
    
    def create_target_table(self, table_name):
        """
        Create a target table with VARIANT column to store CSV rows as JSON.
        Only creates the table if it doesn't exist (insert-only mode).
        
        Table structure:
        - ROW_ID: Auto-incrementing row identifier
        - ROW_DATA: VARIANT column storing each CSV row as JSON
        - FILENAME: STRING column storing the source filename
        - LOAD_TIMESTAMP: Timestamp of when the row was loaded
        
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
                FILENAME STRING,
                FILE_HASH STRING,
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (FILENAME, FILE_HASH)
            """
            
            print(f"\nCREATE TABLE SQL:")
            print(create_table_sql)
            print("=" * 40)
            
            self.cursor.execute(create_table_sql)
            print(f"Table '{table_name}' ready (created if not exists)")
            
            # Ensure clustering is set (for existing tables)
            try:
                cluster_sql = f"ALTER TABLE {table_name} CLUSTER BY (FILENAME, FILE_HASH)"
                self.cursor.execute(cluster_sql)
                print(f"Clustering key set on (FILENAME, FILE_HASH)")
            except ProgrammingError as e:
                # Clustering may already be set or not supported
                if "already has a clustering key" in str(e).lower():
                    print(f"Clustering key already exists on table")
                else:
                    print(f"Note: Could not set clustering key: {e}")
        except ProgrammingError as e:
            print(f"Error creating table: {e}")
            raise

    
    def _read_file_header(self, stage_name, file_path, file_format_name):
        """
        Read the header row from a specific file to get column names.
        
        Args:
            stage_name (str): Name of the stage
            file_path (str): Path to the specific file (relative to stage)
            file_format_name (str): File format name (with SKIP_HEADER=0)
            
        Returns:
            list: List of column names from the header
        """
        # Escape single quotes in file path
        escaped_path = file_path.replace("'", "''")
        
        header_sql = f"""
        SELECT
          t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
          t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
          t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
          t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
          t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
          t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
          t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
          t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80,
          t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90,
          t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100,
          t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
          t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
          t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
          t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
          t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150
        FROM @{stage_name}/{escaped_path} (
            FILE_FORMAT => '{file_format_name}'
        ) AS t
        LIMIT 1
        """
        
        self.cursor.execute(header_sql)
        header_row = self.cursor.fetchone()
        
        if not header_row:
            return None
        
        # Extract column names from header row (remove None values)
        csv_columns = [str(col).strip('"') if col else f'column_{i+1}' 
                      for i, col in enumerate(header_row) if col is not None]
        
        return csv_columns if csv_columns else None
    
    def _process_single_file(self, file_path, stage_name, table_name, file_format_name, data_format, dry_run=False):
        """
        Process a single file with its own header detection.
        Creates its own cursor for thread safety.
        
        Args:
            file_path (str): Path to the file (relative to stage)
            stage_name (str): Name of the stage
            table_name (str): Target table name
            file_format_name (str): File format for header detection (SKIP_HEADER=0)
            data_format (str): File format for data loading (SKIP_HEADER=1)
            dry_run (bool): If True, don't actually load
            
        Returns:
            tuple: (file_path, rows_loaded, error_message or None)
        """
        # Create a new cursor for this thread
        cursor = self.conn.cursor()
        
        try:
            # Read header from this specific file
            escaped_path = file_path.replace("'", "''")
            
            header_sql = f"""
            SELECT
              t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
              t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
              t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
              t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
              t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
              t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
              t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
              t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80,
              t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90,
              t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100,
              t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
              t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
              t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
              t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
              t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150
            FROM @{stage_name}/{escaped_path} (
                FILE_FORMAT => '{file_format_name}'
            ) AS t
            LIMIT 1
            """
            
            cursor.execute(header_sql)
            header_row = cursor.fetchone()
            
            if not header_row:
                return (file_path, 0, "Could not read header")
            
            # Extract column names
            csv_columns = [str(col).strip('"') if col else f'column_{i+1}' 
                          for i, col in enumerate(header_row) if col is not None]
            
            if not csv_columns:
                return (file_path, 0, "No columns detected")
            
            # Build OBJECT_CONSTRUCT for this file's columns
            object_construct_parts = []
            for i, col in enumerate(csv_columns, start=1):
                clean_col = col.replace("'", "''").replace('"', '\\"')
                object_construct_parts.append(f"'{clean_col}', t.${i}")
            object_construct = ', '.join(object_construct_parts)
            
            if dry_run:
                return (file_path, 0, f"[DRY RUN] {len(csv_columns)} columns")
            
            # Build and execute COPY INTO
            # FORCE = TRUE bypasses Snowflake's file loading metadata tracking
            # This is needed because our script does its own deduplication via FILENAME + FILE_HASH
            copy_sql = f"""
            COPY INTO {table_name} (ROW_DATA, FILENAME, FILE_HASH)
            FROM (
                SELECT 
                    OBJECT_CONSTRUCT_KEEP_NULL({object_construct})::VARIANT AS ROW_DATA,
                    REGEXP_REPLACE(METADATA$FILENAME::STRING, '^s3://[^/]+/', '') AS FILENAME,
                    METADATA$FILE_CONTENT_KEY::STRING AS FILE_HASH
                FROM @{stage_name}/{escaped_path} (
                    FILE_FORMAT => '{data_format}'
                ) AS t
            )
            FORCE = TRUE
            """
            
            cursor.execute(copy_sql)
            rows_loaded = cursor.rowcount
            return (file_path, rows_loaded, None)
            
        except Exception as e:
            return (file_path, 0, str(e))
        finally:
            cursor.close()
    
    def ingest_from_stage_auto_columns(self, stage_name, table_name, file_pattern=None, file_format_name=None, file_format_name_data=None, dry_run=False, max_files=None, max_workers=5, start_date='2025-01-01', load_mode='historical'):
        """
        Load CSV data and convert each row to VARIANT JSON without requiring explicit column names.
        Reads the header row from EACH file to get column names dynamically.
        
        Args:
            stage_name (str): Name of the stage
            table_name (str): Name of the target table
            file_pattern (str): Optional file pattern to match
            file_format_name (str): File format for header detection (SKIP_HEADER=0)
            file_format_name_data (str): File format for data loading (SKIP_HEADER=1). If not provided, uses file_format_name.
            dry_run (bool): If True, show what would be loaded without actually loading (default: False)
            max_files (int): Maximum number of files to ingest (for QA/testing). None = no limit.
            max_workers (int): Number of parallel workers for file processing (default: 5)
            start_date (str): Only process files with dates >= this date (format: 'YYYY-MM-DD'). Default: '2025-01-01'
            load_mode (str): 'daily' = only latest file per merchant, 'historical' = all files from start_date. Default: 'historical'
        """
        try:
            if dry_run:
                print(f"\n{'='*50}")
                print(f"  DRY RUN MODE - No data will be loaded")
                print(f"{'='*50}")
            
            # Parse start_date for filtering
            if isinstance(start_date, str):
                cutoff_date = datetime.strptime(start_date, '%Y-%m-%d')
            elif isinstance(start_date, datetime):
                cutoff_date = start_date
            else:
                cutoff_date = datetime(2025, 1, 1)  # Default fallback
            
            print(f"\n=== Per-File Header Detection Mode ===")
            print(f"Stage: {stage_name}")
            print(f"File Pattern: {file_pattern if file_pattern else '(all files)'}")
            print(f"Start Date Filter: {cutoff_date.strftime('%Y-%m-%d')} (only files >= this date)")
            print(f"Load Mode: {load_mode} ({'latest file per merchant only' if load_mode == 'daily' else 'all files from start_date'})")
            
            # Validate file format
            if not file_format_name:
                raise ValueError("file_format_name is required. Please provide a named file format (e.g., 'RAW_DB_PROD.EXT.CSV_FF')")
            
            # Check which files have already been loaded to avoid duplicates
            # Uses FILENAME + FILE_HASH combination for uniqueness
            print(f"\n=== Checking Already Loaded Files ===")
            try:
                # Get distinct filename + hash combinations already in the target table
                check_sql = f"SELECT DISTINCT FILENAME, FILE_HASH FROM {table_name}"
                self.cursor.execute(check_sql)
                loaded_files_raw = {(row[0], row[1]) for row in self.cursor.fetchall()}
                
                # Normalize loaded file paths - remove s3://bucket/ prefix if present
                # Store as (relative_path, file_hash) tuples for comparison
                loaded_file_hashes = set()
                for filename, file_hash in loaded_files_raw:
                    # Remove s3://bucket/ prefix if present to get relative path
                    if filename and filename.startswith('s3://'):
                        # Extract path after bucket name (e.g., s3://bucket/path -> path)
                        parts = filename.split('/', 3)
                        if len(parts) >= 4:
                            relative_path = parts[3]  # Path after s3://bucket/
                        else:
                            relative_path = filename
                    else:
                        relative_path = filename
                    loaded_file_hashes.add((relative_path, file_hash))
                
                print(f"Found {len(loaded_file_hashes)} unique file+hash combination(s) already loaded in table")
                if loaded_file_hashes:
                    sample = list(loaded_file_hashes)[:3]
                    print(f"Sample loaded files: {[(f, h[:8] + '...' if h else None) for f, h in sample]}")
            except ProgrammingError as e:
                # Table might not exist yet or have no data
                print(f"Note: Could not check existing files (table may be empty): {e}")
                loaded_file_hashes = set()
            
            # List files from stage to get available files
            print(f"\n=== Listing Files in Stage ===")
            list_sql = f"LIST @{stage_name}"
            if file_pattern:
                # Convert \. to [.] for literal dot in regex pattern
                pattern_value = file_pattern.replace('\\.', '[.]')
                list_sql += f" PATTERN = '{pattern_value}'"
            
            self.cursor.execute(list_sql)
            stage_files = self.cursor.fetchall()
            
            if not stage_files:
                print(f"No files found in stage matching pattern")
                return 0
            
            # Extract file paths and content hashes
            # LIST returns: name, size, md5 (content hash), last_modified
            # Use the S3 content MD5 (column 2) to detect file changes
            all_file_info = []  # List of (relative_path, full_path, content_hash)
            for file_row in stage_files:
                full_path = file_row[0]
                # Use S3 content MD5 from LIST result (column index 2)
                # This hash changes when file content changes, even if filename stays the same
                content_hash = file_row[2] if len(file_row) > 2 and file_row[2] else None
                
                # Normalize path - remove s3://bucket/ prefix if present
                if full_path.startswith('s3://'):
                    parts = full_path.split('/', 3)
                    if len(parts) >= 4:
                        relative_path = parts[3]
                    else:
                        relative_path = full_path
                else:
                    relative_path = full_path
                
                all_file_info.append((relative_path, full_path, content_hash))
            
            print(f"Found {len(all_file_info)} file(s) in stage matching pattern")
            print(f"Sample file paths: {[f[0] for f in all_file_info[:3]]}")
            
            # Filter out already loaded files (compare relative path + content hash)
            # If same filename has new content (different hash), it will be loaded
            # Also filter by date: only include files with date >= cutoff_date
            def file_passes_date_filter(rel_path):
                """Check if file date is >= cutoff_date."""
                file_date = extract_date_from_filename(rel_path)
                if file_date is None:
                    # If no date in filename, include the file (can't filter)
                    return True
                return file_date >= cutoff_date
            
            new_files_info = [
                (rel_path, full_path, content_hash) 
                for rel_path, full_path, content_hash in all_file_info 
                if (rel_path, content_hash) not in loaded_file_hashes
                and file_passes_date_filter(rel_path)
            ]
            new_files = [rel_path for rel_path, _, _ in new_files_info]
            
            # Count files filtered by date
            files_before_date_filter = len([
                (rel_path, full_path, content_hash) 
                for rel_path, full_path, content_hash in all_file_info 
                if (rel_path, content_hash) not in loaded_file_hashes
            ])
            files_filtered_by_date = files_before_date_filter - len(new_files)
            
            if not new_files:
                print(f"\nNo new files to process.")
                print(f"  - Already loaded (matching filename + hash): {len(loaded_file_hashes)}")
                print(f"  - Filtered by date (before {cutoff_date.strftime('%Y-%m-%d')}): {files_filtered_by_date}")
                return 0
            
            print(f"Found {len(new_files)} new file(s) to load")
            print(f"  - Excluded (already loaded): {len(loaded_file_hashes)}")
            print(f"  - Excluded (before {cutoff_date.strftime('%Y-%m-%d')}): {files_filtered_by_date}")
            
            # Apply daily mode filtering if specified (only latest file per merchant)
            if load_mode == 'daily':
                files_before_daily_filter = len(new_files_info)
                print(f"\n=== Daily Load Mode: Filtering to Latest File per Merchant ===")
                new_files_info = filter_latest_per_merchant(new_files_info)
                new_files = [rel_path for rel_path, _, _ in new_files_info]
                files_filtered_by_daily = files_before_daily_filter - len(new_files)
                print(f"Kept {len(new_files)} file(s) (1 per merchant), excluded {files_filtered_by_daily} older file(s)")
            
            if not new_files:
                print(f"\nNo files to process after filtering.")
                return 0
            
            # Apply max_files limit if specified (for QA/testing)
            if max_files is not None and len(new_files) > max_files:
                print(f"\n[QA MODE] Limiting to first {max_files} files (out of {len(new_files)} available)")
                new_files = new_files[:max_files]
            
            print(f"New files: {new_files[:5]}..." if len(new_files) > 5 else f"New files: {new_files}")
            
            # Use file_format_name_data for COPY INTO (should have SKIP_HEADER=1)
            # Falls back to file_format_name if not provided
            data_format = file_format_name_data if file_format_name_data else file_format_name
            if not data_format:
                raise ValueError("file_format_name is required. Please provide a named file format (e.g., 'RAW_DB_PROD.EXT.CSV_FF')")
            
            print(f"\nUsing file format for header detection: {file_format_name}")
            print(f"Using file format for data loading: {data_format}")
            
            # Process files in parallel with individual header detection
            print(f"\n=== Processing Files in Parallel ===")
            print(f"Total files to process: {len(new_files)}")
            print(f"Parallel workers: {max_workers}")
            print("=" * 40 + "\n")
            
            total_rows_loaded = 0
            files_processed = 0
            files_failed = 0
            
            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all file processing tasks
                future_to_file = {
                    executor.submit(
                        self._process_single_file,
                        file_path,
                        stage_name,
                        table_name,
                        file_format_name,
                        data_format,
                        dry_run
                    ): file_path
                    for file_path in new_files
                }
                
                # Process results as they complete
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result_path, rows_loaded, error = future.result()
                        
                        if error:
                            if error.startswith("[DRY RUN]"):
                                print(f"  {result_path}: {error}")
                            else:
                                print(f"  {result_path}: ERROR - {error}")
                                files_failed += 1
                        else:
                            total_rows_loaded += rows_loaded if rows_loaded is not None else 0
                            files_processed += 1
                            print(f"  {result_path}: {rows_loaded} rows loaded")

                    except Exception as e:
                        print(f"  {file_path}: EXCEPTION - {e}")
                        files_failed += 1
            
            print(f"\n=== Summary ===")
            if dry_run:
                print(f"[DRY RUN] Would load {len(new_files)} file(s) into table '{table_name}'")
                print(f"[DRY RUN] Each file processed with its own header detection")
                print(f"[DRY RUN] No data was actually loaded")
            else:
                print(f"Successfully loaded {total_rows_loaded} total rows from {files_processed} file(s) into table '{table_name}'")
                if files_failed > 0:
                    print(f"Warning: {files_failed} file(s) failed to load")
            print(f"Parallel workers used: {max_workers}")
            return total_rows_loaded
            
        except ProgrammingError as e:
            print(f"Error ingesting data from stage: {e}")
            raise
    
    def list_stage_files(self, stage_name, file_pattern=None):
        """
        List files in the stage.
        
        Args:
            stage_name (str): Name of the stage
            file_pattern (str): Optional file pattern to match
        """
        try:
            # Try INFORMATION_SCHEMA first (works even if SHOW STAGES doesn't)
            try:
                self.cursor.execute(f"""
                    SELECT STAGE_CATALOG, STAGE_SCHEMA, STAGE_NAME, STAGE_URL
                    FROM INFORMATION_SCHEMA.STAGES
                    WHERE STAGE_CATALOG = '{self.config.get("database", "").upper()}'
                    AND STAGE_SCHEMA = '{self.config.get("schema", "").upper()}'
                """)
                stages = self.cursor.fetchall()
                if stages:
                    print(f"\nFound stages in INFORMATION_SCHEMA:")
                    for stage in stages:
                        full_name = f"{stage[0]}.{stage[1]}.{stage[2]}"
                        print(f"  - {full_name} (URL: {stage[3]})")
            except Exception as e:
                print(f"Could not query INFORMATION_SCHEMA.STAGES: {e}")
            
            # Try to list stages using SHOW command
            try:
                self.cursor.execute("SHOW STAGES")
                stages = self.cursor.fetchall()
                if stages:
                    print(f"\nAvailable stages (SHOW STAGES):")
                    for stage in stages[:10]:  # Show first 10
                        # SHOW STAGES returns: created_on, name, database_name, schema_name, ...
                        db = stage[2] if len(stage) > 2 else 'N/A'
                        schema = stage[3] if len(stage) > 3 else 'N/A'
                        name = stage[1] if len(stage) > 1 else 'N/A'
                        print(f"  - {db}.{schema}.{name}")
            except Exception as e:
                print(f"Could not run SHOW STAGES: {e}")
            
            # Now try to list files in the stage
            list_sql = f"LIST @{stage_name}"
            if file_pattern:
                list_sql += f" PATTERN = '{file_pattern}'"
            
            print(f"\nAttempting to list files in stage: {stage_name}")
            self.cursor.execute(list_sql)
            files = self.cursor.fetchall()
            print(f"\nFiles in stage '{stage_name}' ({len(files)} total):")
            if files:
                for file in files[:10]:
                    print(f"  - {file[0]}")
                if len(files) > 10:
                    print(f"  ... and {len(files) - 10} more files")
            else:
                print("  (No files found matching the pattern)")
            return files
        except ProgrammingError as e:
            print(f"Error listing stage files: {e}")
            print(f"\nTroubleshooting:")
            print(f"1. Verify stage name: {stage_name}")
            print(f"2. Try querying: SELECT * FROM INFORMATION_SCHEMA.STAGES")
            print(f"3. Check permissions: SHOW GRANTS ON STAGE {stage_name}")
            print(f"4. Verify database/schema context: SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            raise
    
    def query_table(self, table_name, limit=10):
        """
        Query and display sample data from the target table.
        
        Args:
            table_name (str): Name of the table
            limit (int): Number of rows to display
        """
        try:
            query_sql = f"SELECT * FROM {table_name} LIMIT {limit}"
            self.cursor.execute(query_sql)
            rows = self.cursor.fetchall()
            print(f"\nSample data from '{table_name}':")
            for row in rows:
                print(f"  Row ID: {row[0]}, Data: {row[1]}, Timestamp: {row[2]}")
            return rows
        except ProgrammingError as e:
            print(f"Error querying table: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Snowflake connection closed")


def main():    
    # Snowflake connection configuration
    # IMPORTANT: Set database and schema to match where your STAGE is located
    # If stage is RAW_DB_PROD.EXT.s3_ext_stage, set:
    #   database = 'RAW_DB_PROD'
    #   schema = 'EXT'
    # 

    snowflake_config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT', '[SNOWFLAKE-ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER', '[DBT-SF-SERVICE-USER]'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'ELT_WH_PROD'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'RAW_DB_PROD'),  # Must match stage database
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'EXT'),  # Set to stage schema (EXT), not table schema
        'role': os.getenv('SNOWFLAKE_ROLE', 'ELT_TRANSFORM_PROD'),
        'private_key_path': '/DBT_RSA_KEY.P8',
        'private_key_passphrase': '[DBT_SECRET_KEY]'
    }
    
    # Debug: Show what credentials are being used (password masked)
    print("\n=== Connection Configuration ===")
    print(f"Account: {snowflake_config['account']}")
    print(f"User: {snowflake_config['user']}")
    print(f"Password: {'*' * 10} (hidden)")
    print(f"Warehouse: {snowflake_config['warehouse']}")
    print(f"Database: {snowflake_config['database']}")
    print(f"Schema: {snowflake_config['schema']}")
    print(f"Role: {snowflake_config['role']}")
    print("=" * 35 + "\n")
    
    # ============================================================================
    # CONFIGURATION: Update these values for your environment
    # ============================================================================
    stage_name = 'RAW_PROD.EXT.S3_EXT_STAGE'  # Fully qualified stage name
    
    # ============================================================================
    
    # Initialize ingestion class
    ingestion = SnowflakeS3Ingestion(snowflake_config)
    
    try:
        # Connect to Snowflake
        ingestion.connect()
        
        # List files in stage (optional) - shows files matching the pattern
        ingestion.list_stage_files(stage_name, file_pattern=file_pattern)
        
        # Create target table (if not exists)
        ingestion.create_target_table(table_name)
        
        # Specify named file formats
        # file_format_name: For header detection (needs SKIP_HEADER=0)
        # file_format_name_data: For data loading (needs SKIP_HEADER=1)
        file_format_name = 'RAW_PROD.EXT.CSV_FF'  # SKIP_HEADER=0 for reading header
        file_format_name_data = 'RAW_PROD.EXT.CSV_FF_DATA'  # SKIP_HEADER=1 for loading data
        
        # Set to True to preview what files would be loaded without actually loading
        dry_run = True
         
        # Set max_files to limit ingestion for QA/testing (e.g., 10)
        # Set to None for no limit (production)
        max_files = None
        
        # Number of parallel workers (5 is good for most cases)
        max_workers = 5
        
        # Only process files with dates >= this date (format: YYYY-MM-DD)
        # Files with dates before this will be skipped
        start_date = '2025-01-01'
        
        # Load mode: 'daily' = only latest file per merchant, 'historical' = all files
        load_mode = 'historical'  # Change to 'daily' for daily runs
        
        # Ingest data from stage (column names read automatically from CSV headers)
        ingestion.ingest_from_stage_auto_columns(
            stage_name=stage_name,
            table_name=table_name,
            file_pattern=file_pattern,
            file_format_name=file_format_name,
            file_format_name_data=file_format_name_data,
            dry_run=dry_run,
            max_files=max_files,
            max_workers=max_workers,
            start_date=start_date,
            load_mode=load_mode,
        )
        
        # Query sample data
        ingestion.query_table(table_name, limit=5)
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        ingestion.close()


if __name__ == '__main__':
    main()

