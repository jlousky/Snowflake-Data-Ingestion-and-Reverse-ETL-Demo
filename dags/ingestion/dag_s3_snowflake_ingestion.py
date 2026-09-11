"""
Airflow DAG for ingesting CSV data from S3 into Snowflake using stage mechanism.
This version supports parallel execution of multiple presets.

Each CSV row is converted to JSON and stored as VARIANT data type.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable


# Preset configurations for common ingestion patterns
INGESTION_PRESETS = {
    "DATA_SOURCE_1": {
        "table_name": "RAW_PROD.<source_1>.<raw_table_name>",
        "stage_name": "RAW_PROD.EXT.S3_EXT_STAGE",
        "file_pattern": ".*/data_source_file_01.*\\.csv",
        "file_format": "RAW_PROD.EXT.CSV_FF",
        "file_format_data": "RAW_PROD.EXT.CSV_FF_DATA",
    },
    "DATA_SOURCE_2": {
        "table_name": "RAW_PROD.<source_2>.<raw_table_name>",
        "stage_name": "RAW_PROD.EXT.S3_EXT_STAGE",
        "file_pattern": ".*/data_source_file_02.*\\.csv",
        "file_format": "RAW_PROD.EXT.CSV_FF",
        "file_format_data": "RAW_PROD.EXT.CSV_FF_DATA",
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


def get_snowflake_config():
    """
    Get Snowflake configuration from Airflow Variables or environment variables.
    Supports both password and private key authentication.
    
    Uses SNOWFLAKE_RAW_DATABASE for ingestion (RAW_PROD) instead of
    SNOWFLAKE_DATABASE (ANALYTICS_PROD) which is used for analytics/reverse ETL.
    
    Returns:
        dict: Snowflake connection configuration
    """
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
        'schema': Variable.get('SNOWFLAKE_SCHEMA', default_var=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')),
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
    Get runtime options from DAG params (dry_run, max_files, max_workers).
    
    Returns:
        dict: Runtime options
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
    
    max_files = params.get('max_files', None)
    max_files = int(max_files) if max_files is not None and max_files != '' else None
    
    max_workers = params.get('max_workers', 5)
    max_workers = int(max_workers) if max_workers is not None else 5
    
    start_date = params.get('start_date', '2025-01-01')
    load_mode = params.get('load_mode', 'historical')
    
    return {
        'dry_run': dry_run,
        'max_files': max_files,
        'max_workers': max_workers,
        'start_date': start_date,
        'load_mode': load_mode,
    }


def process_preset(preset_name: str, **context):
    """
    Process a specific preset configuration.
    
    Args:
        preset_name: Name of the preset
    """
    from s3_to_snowflake_ingestion import SnowflakeS3Ingestion

    if preset_name not in INGESTION_PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}")
    
    config = INGESTION_PRESETS[preset_name].copy()
    runtime_opts = get_runtime_options(**context)
    
    table_name = config['table_name']
    stage_name = config['stage_name']
    file_pattern = config.get('file_pattern', '*.csv')
    file_format = config.get('file_format')
    file_format_data = config.get('file_format_data')
    dry_run = runtime_opts['dry_run']
    max_files = runtime_opts['max_files']
    max_workers = runtime_opts['max_workers']
    start_date = runtime_opts['start_date']
    load_mode = runtime_opts['load_mode']
    
    print(f"\n{'='*50}")
    print(f"Processing preset: {preset_name}")
    print(f"{'='*50}")
    print(f"Table: {table_name}")
    print(f"Stage: {stage_name}")
    print(f"File pattern: {file_pattern}")
    print(f"Start date: {start_date}")
    print(f"Load mode: {load_mode}")
    print(f"Dry run: {dry_run}")
    print(f"Max files: {max_files}")
    print(f"Max workers: {max_workers}")
    print(f"{'='*50}\n")
    
    snowflake_config = get_snowflake_config()
    ingestion = SnowflakeS3Ingestion(snowflake_config)
    
    try:
        ingestion.connect()
        
        # List files
        ingestion.list_stage_files(stage_name=stage_name, file_pattern=file_pattern)
        
        # Create table
        ingestion.create_target_table(table_name)
        
        # Ingest data with all parameters
        rows_loaded = ingestion.ingest_from_stage_auto_columns(
            stage_name=stage_name,
            table_name=table_name,
            file_pattern=file_pattern,
            file_format_name=file_format,
            file_format_name_data=file_format_data,
            dry_run=dry_run,
            max_files=max_files,
            max_workers=max_workers,
            start_date=start_date,
            load_mode=load_mode,
        )
        print(f"Successfully loaded {rows_loaded} rows into {table_name}")
        
        # Validate data (skip in dry run mode)
        if not dry_run:
            ingestion.query_table(table_name=table_name, limit=5)
        
        return rows_loaded
            
    except Exception as e:
        print(f"Error processing preset {preset_name}: {e}")
        raise
    finally:
        ingestion.close()


# Create the DAG with parallel tasks for all presets
with DAG(
    's3_to_snowflake_ingestion',
    default_args=default_args,
    description='Ingest CSV data from S3 to Snowflake using stages (Parallel Presets)',
    schedule=None,  # Triggered by master_data_pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 's3', 'ingestion', 'data-pipeline'],
    params={
        'dry_run': False,  # If True, show what would be loaded without actually loading
        'max_files': None,  # Maximum number of files to ingest (for QA/testing)
        'max_workers': 5,  # Number of parallel workers for file processing
        'start_date': '2025-01-01',  # Only process files with dates >= this date (YYYY-MM-DD)
        'load_mode': 'daily',  # 'daily' = latest file per merchant only, 'historical' = all files
    }
) as dag:
    
    # Start task
    start = EmptyOperator(task_id='start')
    
    # End task
    end = EmptyOperator(task_id='end')
    
    # Create a parallel task for each preset
    for preset_name in INGESTION_PRESETS.keys():
        task = PythonOperator(
            task_id=f'process_{preset_name.lower()}',
            python_callable=process_preset,
            op_kwargs={'preset_name': preset_name},
        )
        
        # Set up dependencies: start -> task -> end
        start >> task >> end
