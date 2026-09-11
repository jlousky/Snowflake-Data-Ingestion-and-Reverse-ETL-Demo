# Snowflake Data Ingestion and Reverse ETL Demo

This project contains data pipelines for Snowflake integrations, organized into two main categories:

- **Ingestion** (External Sources → Snowflake): S3 and MongoDB data ingestion into Snowflake
- **Reverse ETL** (Snowflake → External Systems): Push processed data from Snowflake back to MongoDB

## Project Structure

```
data_ingestion_snowflake/
├── dags/
│   ├── ingestion/                    # Data ingestion DAGs (into Snowflake)
│   │   ├── dag_s3_snowflake_ingestion.py
│   │   ├── dag_mongo_snowflake_ingestion.py
│   │   ├── s3_to_snowflake_ingestion.py
│   │   └── mongo_to_snowflake_ingestion.py
│   └── reverse_etl/                  # Reverse ETL DAGs (out of Snowflake)
│       ├── dag_snowflake_to_mongo.py
│       └── snowflake_to_mongo.py
├── keys/
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

# Data Ingestion: S3 to Snowflake

Ingests CSV data from Amazon S3 into Snowflake using Snowflake's external stage mechanism. Each CSV row is converted to JSON and stored as a VARIANT data type in the target table.

## Ingestion Features

- Uses existing Snowflake external stages (stages must be created separately)
- Creates a target table with VARIANT column to store raw CSV data as JSON (insert-only mode - preserves existing data)
- Converts each CSV row to JSON format and stores in VARIANT data type
- Supports file pattern matching for selective loading
- Includes error handling and logging
- **Airflow DAG support**: Includes Airflow DAG for orchestrated data ingestion with runtime parameter support
- **Auto column detection**: Automatically reads CSV column names from headers (no manual specification needed)

## Prerequisites

1. **Snowflake Account**: You need a Snowflake account with appropriate permissions
2. **AWS S3 Access**: Access to the S3 bucket containing CSV files
3. **Python 3.7+**: Python 3.7 or higher
4. **External Stages**: External stages must already exist in Snowflake (this script does not create them)
5. **Required Permissions**:
   - CREATE TABLE privilege
   - INSERT privilege on target table
   - USAGE privilege on database and schema
   - USAGE privilege on existing stages

## Installation

1. Install required Python packages:
```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set the following environment variables or modify the `main()` function:

```bash
export SNOWFLAKE_ACCOUNT='your_account_identifier'
export SNOWFLAKE_USER='your_username'
export SNOWFLAKE_PASSWORD='your_password'
export SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
export SNOWFLAKE_DATABASE='YOUR_DATABASE'
export SNOWFLAKE_SCHEMA='PUBLIC'
export SNOWFLAKE_ROLE='ACCOUNTADMIN'

```

**Note**: CSV column names are automatically detected from the CSV header row - no manual configuration needed.

## Usage

### Basic Usage

```python
from s3_to_snowflake_ingestion import SnowflakeS3Ingestion

# Configure Snowflake connection
snowflake_config = {
    'account': 'your_account',
    'user': 'your_username',
    'password': 'your_password',
    'warehouse': 'COMPUTE_WH',
    'database': 'YOUR_DATABASE',
    'schema': 'PUBLIC',
    'role': 'ACCOUNTADMIN'
}

# Initialize and run ingestion
ingestion = SnowflakeS3Ingestion(snowflake_config)
ingestion.connect()

# List files in stage (optional)
ingestion.list_stage_files('S3_CSV_STAGE', file_pattern='*.csv')

# Create target table (if not exists)
ingestion.create_target_table('RAW_CSV_DATA')

# Ingest data (column names read automatically from CSV headers)
ingestion.ingest_from_stage_auto_columns(
    stage_name='S3_CSV_STAGE',
    table_name='RAW_CSV_DATA',
    file_pattern='*.csv'
)

ingestion.close()
```

### Running the Script

```bash
python s3_to_snowflake_ingestion.py
```

## Airflow DAG Usage

### Quick Start with Airflow

1. **Set up Airflow Variables** (Admin → Variables):
   - `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, etc.
   - `SNOWFLAKE_TABLE_CONFIGS`: JSON array of table configurations

2. **Copy DAG files to Airflow dags folder**:
   ```bash
   cp dag_snowflake_ingestion.py $AIRFLOW_HOME/dags/
   cp s3_to_snowflake_ingestion.py $AIRFLOW_HOME/dags/
   ```

3. **Trigger the DAG with runtime parameters** (recommended):
   - In Airflow UI, click "Trigger DAG w/ config"
   - Pass configuration:
   ```json
   {
       "table_name": "RAW_CUSTOMERS",
       "stage_name": "S3_CUSTOMERS_STAGE",
       "file_pattern": "customers_*.csv"
   }
   ```

   Or set `SNOWFLAKE_TABLE_CONFIGS` Airflow Variable as fallback.

### Example Table Configuration

```json
[
    {
        "table_name": "RAW_CUSTOMERS",
        "stage_name": "S3_CUSTOMERS_STAGE",
        "file_pattern": "customers_*.csv"
    }
]
```

**Note**: `csv_columns` is no longer needed - column names are automatically read from CSV headers.

## How It Works

1. **Stage Usage**: Uses an existing external stage in Snowflake (must be created separately)
2. **Table Creation**: Creates a target table if it doesn't exist (insert-only mode, preserves existing data):
   - `ROW_ID`: Auto-incrementing row identifier
   - `ROW_DATA`: VARIANT column storing each CSV row as JSON
   - `LOAD_TIMESTAMP`: Timestamp of when the row was loaded
3. **Data Ingestion**: Uses `COPY INTO` command to:
   - Read CSV header row to automatically detect column names
   - Read CSV files from the existing stage
   - Convert each row to a JSON object using `OBJECT_CONSTRUCT` with detected column names
   - Insert/append the JSON data to the VARIANT column (does not replace existing data)

## Table Structure

The target table has the following structure:

```sql
CREATE TABLE RAW_CSV_DATA (
    ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    ROW_DATA VARIANT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
```

## Querying the Data

After ingestion, you can query the VARIANT data:

```sql
-- View raw JSON data
SELECT ROW_ID, ROW_DATA, LOAD_TIMESTAMP
FROM RAW_CSV_DATA;

-- Extract specific fields from VARIANT
SELECT 
    ROW_ID,
    ROW_DATA:id::STRING AS id,
    ROW_DATA:name::STRING AS name,
    ROW_DATA:email::STRING AS email,
    ROW_DATA:age::INTEGER AS age,
    ROW_DATA:city::STRING AS city
FROM RAW_CSV_DATA;
```

## Methods Available

- `connect()`: Establish connection to Snowflake
- `create_target_table()`: Create target table with VARIANT column (if not exists)
- `ingest_from_stage_auto_columns()`: Ingest data with automatic column detection from CSV headers (recommended)
- `ingest_from_stage_simple()`: Ingest data with explicit column mapping (requires csv_columns parameter)
- `ingest_from_stage_using_parse_json()`: Alternative ingestion method
- `list_stage_files()`: List files in the stage
- `query_table()`: Query and display sample data
- `close()`: Close Snowflake connection

## Error Handling

The script includes error handling for:
- Connection failures
- Table creation errors
- Data ingestion errors
- Stage access errors

## Security Best Practices

1. **Use Environment Variables**: Store credentials in environment variables, not in code
2. **IAM Roles**: Consider using IAM roles instead of access keys for S3 access
3. **Snowflake Key Pair Authentication**: Consider using key pair authentication instead of passwords
4. **Least Privilege**: Grant only necessary permissions to the Snowflake user

## Troubleshooting

1. **Connection Issues**: Verify Snowflake account, user, and password
2. **Stage Not Found**: Ensure external stages are created in Snowflake before running
3. **No Data Loaded**: Verify file pattern matches files in S3, check CSV format, verify stage has access to S3
4. **CSV Header Issues**: Ensure CSV files have header rows (required for auto-column detection)
5. **Runtime Parameters**: When using Airflow, ensure `table_name` and `stage_name` are provided via config or Airflow Variable

---

# Reverse ETL: Snowflake to MongoDB

Performs UPDATE operations from Snowflake to MongoDB's transactions collection. This is used to push processed/reconciled data back to the operational MongoDB database.

## Reverse ETL Features

- Updates existing MongoDB records (matched by `transaction_id`)
- Does NOT insert new records (update-only mode)
- Uses parallel processing for high throughput
- Configurable batch size and worker count
- **Target selection**: Choose between `dev` and `prod` MongoDB environments
- **Preset configurations**: Pre-defined configurations for common use cases
- Airflow DAG with validation and execution tasks

## Reverse ETL Usage

### Using the Airflow DAG

1. Copy DAG files to Airflow:
   ```bash
   cp dags/reverse_etl/* $AIRFLOW_HOME/dags/
   ```

2. Set Airflow Variables (optional):
   - `snowflake_to_mongo_preset`
   - `snowflake_to_mongo_batch_size`
   - `snowflake_to_mongo_parallel_workers`
   - `snowflake_to_mongo_where_clause`

3. Trigger the DAG with parameters:
   ```json
   {"target": "prod"}
   ```
