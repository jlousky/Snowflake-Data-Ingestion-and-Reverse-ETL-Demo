"""
Snowflake to MongoDB Reverse ETL DAG
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


prod_defaults = {
    "connection_string": "mongodb://localhost:27017",
    "database": "production_db"
}

dev_defaults = {
    "connection_string": "mongodb://localhost:27017",
    "database": "development_db"
}

PRESETS = {
    "transactions": {
        "description": "Update transactions",
        "view": "TRANSACTIONS_VW",
        "database": "ANALYTICS_PROD",
        "schema": "[SOURCE_SCHEMA]",
        "collection": "transactions",
        "match_field": "TRANSACTION_ID",
        "mongo_match_field": "transactionId",
        "field_mapping": {
            "AMOUNT": "Amount",
        }
    }
}


# =============================================================================
# Configuration Helper Functions
# =============================================================================

def get_snowflake_config():

    return {
        'account': Variable.get('SNOWFLAKE_ACCOUNT', default_var=os.getenv('SNOWFLAKE_ACCOUNT', '[SNOWFLAKE_ACCOUNT]')),
        'user': Variable.get('SNOWFLAKE_USER', default_var=os.getenv('SNOWFLAKE_USER', '[SNOWFLAKE-SERVICE-USER]')),
        'warehouse': 'ELT_WH_PROD',
        'database': 'ANALYTICS_PROD',
        'schema': '[SCHEMA]',
        'role': 'ELT_TRANSFORM_PROD',
        'private_key_path': Variable.get('SNOWFLAKE_PRIVATE_KEY_PATH', 
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', '/opt/airflow/keys/DBT_PROD_RSA_KEY.p8')),
        'private_key': Variable.get('SNOWFLAKE_PRIVATE_KEY',
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY')),
        'private_key_passphrase': Variable.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE',
            default_var=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '[DBT_SECRET_KEY]')),
    }


def get_mongo_config(target=None):
    """
    Get MongoDB configuration from Airflow Variables or environment variables.
    
    Args:
        target: Target environment ('dev' or 'prod'). If None, defaults to 'prod'.
    
    Returns:
        dict: MongoDB connection configuration
    """
    # Default to prod if not provided
    if target is None:
        target = 'prod'
    target = str(target).lower()
    

    if target == 'prod':
        # Use prod variables with prod defaults
        return {
            'connection_string': Variable.get('MONGO_CONNECTION_STRING_PROD', 
                default_var=os.getenv('MONGO_CONNECTION_STRING_PROD', prod_defaults['connection_string'])),
            'database': Variable.get('MONGODB_DATABASE_PROD', 
                default_var=os.getenv('MONGODB_DATABASE_PROD', prod_defaults['database'])),
            'target': 'prod',
        }
    else:
        # Use dev variables with dev defaults
        return {
            'connection_string': Variable.get('MONGO_CONNECTION_STRING_DEV', 
                default_var=os.getenv('MONGO_CONNECTION_STRING_DEV', dev_defaults['connection_string'])),
            'database': Variable.get('MONGODB_DATABASE_DEV', 
                default_var=os.getenv('MONGODB_DATABASE_DEV', dev_defaults['database'])),
            'target': 'dev',
        }


def set_config_from_airflow_variables(target=None):
    """
    Set environment variables from Airflow Variables to configure snowflake_to_mongo.py.
    
    Args:
        target: MongoDB target environment ('dev' or 'prod').
                - Snowflake always uses PROD (ANALYTICS_DB_PROD)
                - MongoDB uses dev or prod based on this parameter
    """
    config = get_snowflake_config()
    mongo_config = get_mongo_config(target=target)
    
    # Set Snowflake environment variables
    if config.get('account'):
        os.environ['SNOWFLAKE_ACCOUNT'] = config['account']
    if config.get('user'):
        os.environ['SNOWFLAKE_USER'] = config['user']
    if config.get('warehouse'):
        os.environ['SNOWFLAKE_WAREHOUSE'] = config['warehouse']
    if config.get('database'):
        os.environ['SNOWFLAKE_DATABASE'] = config['database']
    if config.get('schema'):
        os.environ['SNOWFLAKE_SCHEMA'] = config['schema']
    if config.get('role'):
        os.environ['SNOWFLAKE_ROLE'] = config['role']
    if config.get('private_key_path'):
        os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'] = config['private_key_path']
    if config.get('private_key_passphrase'):
        os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = config['private_key_passphrase']
    
    # Set MongoDB environment variables (set both names for compatibility)
    if mongo_config.get('connection_string'):
        os.environ['MONGO_CONNECTION_STRING'] = mongo_config['connection_string']
        os.environ['MONGODB_URI'] = mongo_config['connection_string']  # For snowflake_to_mongo.py
    if mongo_config.get('database'):
        os.environ['MONGO_DATABASE'] = mongo_config['database']
        os.environ['MONGODB_DATABASE'] = mongo_config['database']  # For snowflake_to_mongo.py


# =============================================================================
# DAG Default Arguments
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# =============================================================================
# Helper Functions (imported from snowflake_to_mongo.py)
# =============================================================================
# All helper functions are imported from snowflake_to_mongo.py


# =============================================================================
# Runtime Configuration Helpers
# =============================================================================

def get_target(**context):
    """
    Get MongoDB target from DAG params.
    
    Args:
        **context: Airflow context dictionary
    
    Returns:
        str: Target name ('dev' or 'prod') for MongoDB destination
    """
    params = context.get('params', {})
    dag_run = context.get('dag_run')
    
    # Merge dag_run.conf into params (dag_run.conf takes precedence)
    if dag_run and dag_run.conf:
        params = {**params, **dag_run.conf}
    
    # Get target from params or default to 'prod'
    target = params.get('target', 'prod')
    
    # Normalize to lowercase
    target = str(target).lower() if target else 'prod'
    
    # Validate target
    if target not in ['dev', 'prod']:
        print(f"WARNING: Invalid target '{target}', defaulting to 'prod'")
        target = 'prod'
    
    return target


# =============================================================================
# Main Task Functions
# =============================================================================


def validate_connections(**context):
    """Validate Snowflake and MongoDB connections before processing."""
    _dag_file_dir = Path(__file__).parent
    if str(_dag_file_dir) not in sys.path:
        sys.path.insert(0, str(_dag_file_dir))
    from snowflake_to_mongo import get_snowflake_connection, get_mongodb_connection

    # Get target from DAG params (controls MongoDB destination)
    target = get_target(**context)
    
    # Set environment variables from Airflow Variables
    set_config_from_airflow_variables(target=target)
    
    # Get preset from Airflow Variable (default: transactions_reconciled)
    preset_name = Variable.get("snowflake_to_mongo_preset", default_var="transactions")
    
    # Show which environments are being used
    sf_config = get_snowflake_config()
    mongo_config = get_mongo_config(target=target)
    print(f"\nSnowflake: {sf_config['database']}.{sf_config['schema']} (always PROD)")
    print(f"MongoDB Target: {mongo_config['target'].upper()}")
    print(f"MongoDB Database: {mongo_config['database']}")
    
    if preset_name not in PRESETS:
        raise ValueError(f"Invalid preset: {preset_name}. Available presets: {list(PRESETS.keys())}")
    
    preset = PRESETS[preset_name]
    
    print("=" * 60)
    print("VALIDATING CONNECTIONS")
    print("=" * 60)
    print(f"Using preset: {preset_name}")

    # Validate Snowflake connection
    print("\nConnecting to Snowflake...")
    # Show the key path being used (for debugging)
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/opt/airflow/keys/DBT_RSA_KEY.P8")
    print(f"Using RSA key path: {key_path}")
    
    # Debug: Check if directory and file exist
    key_dir = os.path.dirname(key_path)
    if os.path.exists(key_dir):
        print(f"Key directory exists: {key_dir}")
        try:
            files = os.listdir(key_dir)
            print(f"Files in key directory: {files}")
        except Exception as e:
            print(f"Could not list directory contents: {e}")
    else:
        print(f"WARNING: Key directory does not exist: {key_dir}")
        # Check if /opt/airflow exists and what's in it
        if os.path.exists("/opt/airflow"):
            try:
                airflow_dirs = [d for d in os.listdir("/opt/airflow") if os.path.isdir(os.path.join("/opt/airflow", d))]
                print(f"Directories in /opt/airflow: {airflow_dirs}")
            except Exception as e:
                print(f"Could not list /opt/airflow: {e}")
    
    if not os.path.exists(key_path):
        print(f"WARNING: Key file not found at {key_path}")
        print("TIP: Set Airflow Variable 'snowflake_private_key_path' to the correct path")
        print("TIP: Verify docker volume mount: ${AIRFLOW_PROJ_DIR:-.}/keys:/opt/airflow/keys:ro")
    
    try:
        sf_conn = get_snowflake_connection()
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute("SELECT CURRENT_VERSION()")
        version = sf_cursor.fetchone()[0]
        print(f"✓ Connected to Snowflake (version: {version})")
        print(f"  Source: {preset.get('schema', 'RECON_OPS')}.{preset['view']}")
        sf_cursor.close()
        sf_conn.close()
    except Exception as e:
        raise Exception(f"Snowflake connection failed: {e}")

    # Validate MongoDB connection
    print("\nConnecting to MongoDB...")
    try:
        mongo_client, mongo_collection = get_mongodb_connection(collection_name=preset["collection"])
        mongo_client.admin.command("ping")
        print(f"✓ Connected to MongoDB: {preset['collection']}")
        mongo_client.close()
    except Exception as e:
        raise Exception(f"MongoDB connection failed: {e}")

    print("\n✓ All connections validated successfully")


def run_reverse_etl(**context):
    """
    Run the UPDATE operation from Snowflake to MongoDB (no inserts).

    Uses the run_update function from snowflake_to_mongo.py with preset configuration.
    """
    _dag_file_dir = Path(__file__).parent
    if str(_dag_file_dir) not in sys.path:
        sys.path.insert(0, str(_dag_file_dir))
    from snowflake_to_mongo import run_update

    # Get target from DAG params (controls MongoDB destination)
    target = get_target(**context)
    
    # Set environment variables from Airflow Variables
    set_config_from_airflow_variables(target=target)
    
    # Get configuration from Airflow Variables (with defaults)
    preset_name = Variable.get("snowflake_to_mongo_preset", default_var="transactions")
    batch_size = int(Variable.get("snowflake_to_mongo_batch_size", default_var=5000))
    parallel_workers = int(Variable.get("snowflake_to_mongo_parallel_workers", default_var=4))
    where_clause = Variable.get("snowflake_to_mongo_where_clause", default_var=None)

    # Validate preset
    if preset_name not in PRESETS:
        raise ValueError(f"Invalid preset: {preset_name}. Available presets: {list(PRESETS.keys())}")
    
    preset = PRESETS[preset_name]
    
    # Get configs to show info
    sf_config = get_snowflake_config()
    mongo_config = get_mongo_config(target=target)
    
    print("=" * 60)
    print("SNOWFLAKE TO MONGODB REVERSE ETL (UPDATE ONLY)")
    print("=" * 60)
    print(f"\nUsing preset: {preset_name}")
    print(f"Snowflake Source: {sf_config['database']}.{preset.get('schema', '[TARGET_SCHEMA]')}.{preset['view']} (always PROD)")
    print(f"MongoDB Target: {mongo_config['target'].upper()}")
    print(f"MongoDB Database: {mongo_config['database']}")
    print(f"Target: {preset['collection']}")
    print(f"Match: {preset['match_field']} -> {preset['mongo_match_field']}")
    print(f"Fields: {', '.join(f'{k}->{v}' for k, v in preset['field_mapping'].items())}")
    print(f"Batch Size: {batch_size}")
    print(f"Parallel Workers: {parallel_workers}")
    if where_clause:
        print(f"Filter: {where_clause}")
    print(f"Started at: {datetime.now()}")

    # Call run_update with preset configuration
    stats = run_update(
        batch_size=batch_size,
        where_clause=where_clause,
        dry_run=False,
        parallel_workers=parallel_workers,
        table_name=preset["view"],  # Use "view" from preset
        database_name=preset.get("database"),  # Database from preset (if specified)
        schema_name=preset.get("schema"),
        collection_name=preset["collection"],
        field_mapping=preset["field_mapping"],
        match_field=preset["match_field"],
        mongo_match_field=preset["mongo_match_field"]
    )

    # Print summary
    print("\n" + "=" * 60)
    print("UPDATE SUMMARY")
    print("=" * 60)
    print(f"Snowflake records processed: {stats.get('snowflake_records', 0):,}")
    print(f"Batches processed:           {stats.get('batches_processed', 0):,}")
    print(f"MongoDB documents matched:   {stats.get('matched', 0):,}")
    print(f"MongoDB documents modified:  {stats.get('modified', 0):,}")
    print(f"Errors:                      {stats.get('errors', 0):,}")

    if stats.get("start_time") and stats.get("end_time"):
        duration = stats["end_time"] - stats["start_time"]
        print(f"\nDuration: {duration}")

    print(f"Completed at: {stats.get('end_time', datetime.now())}")

    # Push stats to XCom for downstream tasks
    context["ti"].xcom_push(key="etl_stats", value=stats)

    if stats.get("errors", 0) > 0:
        raise Exception(f"ETL completed with {stats.get('errors', 0)} errors")

    return stats


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="snowflake_to_mongo_reverse_etl",
    default_args=default_args,
    description="Reverse ETL: Update MongoDB documents from Snowflake (UPDATE only, no inserts)",
    schedule=None,  # Triggered by master_data_pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reverse-etl", "snowflake", "mongodb", "transactions"],
    doc_md=__doc__,
    params={  # type: ignore[arg-type]
        'target': 'prod', 
    }
) as dag:

    # Task 1: Validate connections
    validate_connections_task = PythonOperator(
        task_id="validate_connections",
        python_callable=validate_connections,
        provide_context=True,
    )

    # Task 2: Run the reverse ETL
    run_reverse_etl_task = PythonOperator(
        task_id="run_reverse_etl",
        python_callable=run_reverse_etl,
        provide_context=True,
    )

    # Define task dependencies
    _ = validate_connections_task >> run_reverse_etl_task
