"""
Airflow DAG for exporting MongoDB Atlas data to Snowflake.
This version supports parallel execution of multiple collection presets.

Each MongoDB document is stored as VARIANT data type in Snowflake.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable


# Preset configurations for MongoDB collection exports
# Add new presets here to export additional collections
MONGO_EXPORT_PRESETS = {
    "TRANSACTIONS": {
        "collection_name": "transactions",
        "table_name": "TRANSACTIONS",
        "schema": "SOURCE_DB_OPS",
        "query_filter": {"valueDate": {"$gte": 1735689600000}},  # Jan 1, 2025
        "fields_to_extract": [
            '_id', 'transactionId', 'transactionDate', 'netAmount', 'date',
            'store', 'transactionAmount', 'currency'
        ],
    },
    "STORES": {
        "collection_name": "stores",
        "table_name": "STORES",
        "schema": "SOURCE_DB_OPS",
        "query_filter": None,
        "fields_to_extract": [
            '_id', 'address', 'merchantId', 'storeId', 'storeName'
        ],
    },
    "MERCHANTS": {
        "collection_name": "merchants",
        "table_name": "MERCHANTS",
        "schema": "SOURCE_DB_OPS",
        "query_filter": None,
        "fields_to_extract": [
            '_id', 'brand', 'isActive', 'type', 'merchantId'
        ],
    },
}


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_mongo_config():
    """
    Get MongoDB PROD configuration from Airflow Variables or environment variables.
    Always uses PROD environment for ingestion.
    
    Returns:
        dict: MongoDB connection configuration
    """
    return {
        'connection_string': Variable.get('MONGO_CONNECTION_STRING_PROD', 
            default_var=os.getenv('MONGO_CONNECTION_STRING_PROD')),
        'database': Variable.get('MONGO_DATABASE_PROD', 
            default_var=os.getenv('MONGO_DATABASE_PROD')),
    }


def get_snowflake_config(schema_override=None):
    """
    Get Snowflake configuration from Airflow Variables or environment variables.
    Supports both password and private key authentication.
    
    Uses SNOWFLAKE_RAW_DATABASE for ingestion (RAW_PROD) instead of
    SNOWFLAKE_DATABASE (ANALYTICS_PROD) which is used for analytics/reverse ETL.
    
    Args:
        schema_override: Optional schema name to override default
    
    Returns:
        dict: Snowflake connection configuration
    """
    schema = schema_override or Variable.get('SNOWFLAKE_SCHEMA', 
        default_var=os.getenv('SNOWFLAKE_SCHEMA', 'EXT'))
    
    # Use SNOWFLAKE_RAW_DATABASE for ingestion, fall back to SNOWFLAKE_DATABASE
    database = Variable.get('SNOWFLAKE_RAW_DATABASE', 
        default_var=os.getenv('SNOWFLAKE_RAW_DATABASE',
            Variable.get('SNOWFLAKE_DATABASE', default_var=os.getenv('SNOWFLAKE_DATABASE'))))
    
    return {
        'account': Variable.get('SNOWFLAKE_ACCOUNT', default_var=os.getenv('SNOWFLAKE_ACCOUNT')),
        'user': Variable.get('SNOWFLAKE_USER', default_var=os.getenv('SNOWFLAKE_USER')),
        'password': Variable.get('SNOWFLAKE_PASSWORD', default_var=os.getenv('SNOWFLAKE_PASSWORD')),
        'warehouse': Variable.get('SNOWFLAKE_WAREHOUSE', default_var=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')),
        'database': database,
        'schema': schema,
        'role': Variable.get('SNOWFLAKE_ROLE', default_var=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')),
        'private_key_path': Variable.get('SNOWFLAKE_PRIVATE_KEY_PATH', 
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')),
        'private_key': Variable.get('SNOWFLAKE_PRIVATE_KEY',
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY')),
        'private_key_passphrase': Variable.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE',
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')),
    }


def get_runtime_options(**context):
    """
    Get runtime options from DAG params.
    
    Returns:
        dict: Runtime options (dry_run, batch_size, workers, upsert, limit, no_parallel)
    """
    params = context.get('params', {})
    dag_run = context.get('dag_run')
    
    # Merge dag_run.conf into params (dag_run.conf takes precedence)
    if dag_run and dag_run.conf:
        params = {**params, **dag_run.conf}
    
    # Parse runtime options (convert types as Airflow params may be strings)
    dry_run = params.get('dry_run', False)
    if isinstance(dry_run, str):
        dry_run = dry_run.lower() in ('true', '1', 'yes')
    
    upsert = params.get('upsert', True)
    if isinstance(upsert, str):
        upsert = upsert.lower() in ('true', '1', 'yes')
    
    no_parallel = params.get('no_parallel', False)
    if isinstance(no_parallel, str):
        no_parallel = no_parallel.lower() in ('true', '1', 'yes')
    
    batch_size = params.get('batch_size', 25000)
    batch_size = int(batch_size) if batch_size is not None else 25000
    
    workers = params.get('workers', 8)
    workers = int(workers) if workers is not None else 8
    
    limit = params.get('limit', None)
    limit = int(limit) if limit is not None and limit != '' else None
    
    return {
        'dry_run': dry_run,
        'batch_size': batch_size,
        'workers': workers,
        'upsert': upsert,
        'no_parallel': no_parallel,
        'limit': limit,
    }


def process_preset(preset_name: str, **context):
    """
    Process a specific MongoDB export preset configuration.
    
    Args:
        preset_name: Name of the preset (e.g., TRANSACTIONS)
    """
    from mongo_to_snowflake_ingestion import MongoAtlasSnowflakeExport

    if preset_name not in MONGO_EXPORT_PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}")
    
    config = MONGO_EXPORT_PRESETS[preset_name].copy()
    runtime_opts = get_runtime_options(**context)
    
    collection_name = config['collection_name']
    table_name = config['table_name']
    schema = config.get('schema')
    query_filter = config.get('query_filter')
    fields_to_extract = config.get('fields_to_extract')
    
    dry_run = runtime_opts['dry_run']
    batch_size = runtime_opts['batch_size']
    workers = runtime_opts['workers']
    upsert = runtime_opts['upsert']
    no_parallel = runtime_opts['no_parallel']
    limit = runtime_opts['limit']
    
    print(f"\n{'='*60}")
    print(f"Processing MongoDB Export Preset: {preset_name}")
    print(f"{'='*60}")
    print(f"Collection: {collection_name}")
    print(f"Target Table: {table_name}")
    print(f"Schema: {schema}")
    print(f"Batch Size: {batch_size:,}")
    print(f"Upsert Mode: {'Enabled' if upsert else 'Disabled'}")
    print(f"Parallel Mode: {'Disabled' if no_parallel else f'Enabled ({workers} workers)'}")
    print(f"Dry Run: {'YES - No data will be inserted' if dry_run else 'No'}")
    print(f"Document Limit: {limit:,}" if limit else "Document Limit: No limit")
    if query_filter:
        print(f"Query Filter: {query_filter}")
    if fields_to_extract:
        print(f"Fields: {len(fields_to_extract)} fields specified")
    else:
        print(f"Fields: All fields")
    print(f"{'='*60}\n")
    
    mongo_config = get_mongo_config()
    snowflake_config = get_snowflake_config(schema_override=schema)
    
    print(f"MongoDB Database: {mongo_config.get('database')} (PROD)")
    print(f"Snowflake Database: {snowflake_config.get('database')}")
    
    exporter = MongoAtlasSnowflakeExport(
        mongo_config, 
        snowflake_config, 
        num_workers=workers
    )
    
    try:
        # Connect to MongoDB and Snowflake
        print("Step 1/4: Establishing connections...")
        exporter.connect_mongo()
        exporter.connect_snowflake()
        print("Connections established successfully\n")
        
        # Create target table
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
            upsert=upsert,
            parallel=not no_parallel,
            dry_run=dry_run,
            limit=limit
        )
        print("Export completed\n")
        
        print(f"Successfully exported {rows_inserted:,} documents to {table_name}")
        return rows_inserted
        
    except Exception as e:
        print(f"Error processing preset {preset_name}: {e}")
        raise
    finally:
        print("Step 4/4: Closing connections...")
        exporter.close()
        print("All connections closed\n")


# Create the DAG with parallel tasks for all presets
with DAG(
    'mongo_to_snowflake_ingestion',
    default_args=default_args,
    description='Export MongoDB Atlas collections to Snowflake (Parallel Presets)',
    schedule=None,  # Triggered by master_data_pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'mongodb', 'ingestion', 'data-pipeline'],
    params={  # type: ignore[arg-type]
        'dry_run': False,      # If True, show what would be done without inserting
        'batch_size': 25000,   # Documents per batch
        'workers': 8,          # Number of parallel worker threads
        'upsert': True,        # Update existing records by document ID
        'no_parallel': False,  # Set True to disable parallel processing
        'limit': None,         # Limit documents to export (for testing)
    }
) as dag:
    
    # Start task
    start = EmptyOperator(task_id='start')
    
    # End task
    end = EmptyOperator(task_id='end')
    
    # Create a parallel task for each preset
    for preset_name in MONGO_EXPORT_PRESETS.keys():
        task = PythonOperator(
            task_id=f'process_{preset_name.lower()}',
            python_callable=process_preset,
            op_kwargs={'preset_name': preset_name},
        )
        
        # Set up dependencies: start -> task -> end
        _ = start >> task >> end
