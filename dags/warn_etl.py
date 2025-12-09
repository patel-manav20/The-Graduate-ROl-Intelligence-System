"""
WARN ETL Pipeline
=================
Extracts, transforms, and loads WARN (Worker Adjustment and Retraining Notification) data
from CSV into Snowflake for analysis of layoffs and closures.

Schedule: Daily
Purpose: WARN layoff and closure data for ROI forecasts
"""

from datetime import datetime, timedelta
import logging
import os
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DATABASE = "USER_DB_HYENA"
SNOWFLAKE_SCHEMA = "RAW"  
WARN_TABLE_NAME = "warn_events"


INPUT_FILE_PATH = "/opt/airflow/dags/data/warn_events.csv"
OUTPUT_FILE_PATH = "/opt/airflow/dags/data/transformed/warn_events_transformed.csv"

default_args = {
    'owner': 'Manav Patel',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email': [],
}


def get_snowflake_executor():
    """
    Helper function to get Snowflake connection and executor.
    Returns (connection, executor) tuple for executing SQL statements.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False  # Enable transaction control (BEGIN/COMMIT/ROLLBACK)
    executor = conn.cursor()
    return conn, executor


@dag(
    dag_id='warn_etl',
    default_args=default_args,
    description='ETL pipeline for WARN layoff and closure data',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['warn', 'layoffs', 'etl', 'snowflake'],
)
def warn_etl_pipeline():
    """
    Main ETL pipeline DAG using TaskFlow API
    """
    
    @task(task_id='extract_warn_data')
    def extract_warn_data() -> str:
        logger = logging.getLogger(__name__)
        logger.info(f"Extracting WARN data from {INPUT_FILE_PATH}")
        
        # Check if file exists, try local path as fallback
        if not os.path.exists(INPUT_FILE_PATH):
            local_path = "/Users/manavnayanbhaipatel/Desktop/SJSU/DATA - 226/Group Project/warn_events.csv"
            if os.path.exists(local_path):
                logger.info(f"Using local path: {local_path}")
                return local_path
            else:
                raise FileNotFoundError(f"WARN CSV file not found at {INPUT_FILE_PATH} or {local_path}")
        else:
            logger.info(f"Found WARN CSV file at {INPUT_FILE_PATH}")
            return INPUT_FILE_PATH
    
    @task(task_id='transform_warn_data')
    def transform_warn_data(input_path: str) -> str:
        logger = logging.getLogger(__name__)
        logger.info("Transforming WARN data")
        
        
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} rows from {input_path}")
        logger.info(f"Columns: {list(df.columns)}")
        
        
        rename_map = {
            "State": "STATE",
            "Company": "COMPANY",
            "City": "CITY",
            "Number of Workers": "NUMBER_OF_WORKERS",
            "WARN Received Date": "WARN_RECEIVED_DATE",
            "Effective Date": "EFFECTIVE_DATE",
            "Closure / Layoff": "CLOSURE_LAYOFF",
            "LayoffDuration": "LAYOFFDURATION",
            "Region": "REGION",
            "County": "COUNTY",
            "Industry": "INDUSTRY",
        }
        
        df = df.rename(columns=rename_map)
        
        # Required columns for Snowflake table
        required_cols = [
            "STATE", "COMPANY", "CITY", "NUMBER_OF_WORKERS",
            "WARN_RECEIVED_DATE", "EFFECTIVE_DATE", "CLOSURE_LAYOFF",
            "LAYOFFDURATION", "REGION", "COUNTY", "INDUSTRY"
        ]
        
        
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        df = df[required_cols]
        
        df["NUMBER_OF_WORKERS"] = (
            df["NUMBER_OF_WORKERS"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        
        for col in ["WARN_RECEIVED_DATE", "EFFECTIVE_DATE"]:
            dt = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
            df[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
            df[col] = df[col].replace("NaT", None)
        
        
        df = df.where(pd.notnull(df), None)
        
        logger.info(f"Transformed {len(df)} rows")
        logger.info(f"Final columns: {list(df.columns)}")
        
        # Save transformed data to output file
        output_dir = os.path.dirname(OUTPUT_FILE_PATH)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(OUTPUT_FILE_PATH, index=False)
        logger.info(f"Transformed data saved to {OUTPUT_FILE_PATH}")
        
        return OUTPUT_FILE_PATH
    
    @task(task_id='load_warn_to_snowflake')
    def load_warn_to_snowflake(file_path: str) -> dict:
        """
        Load WARN Data to Snowflake using staging table and MERGE.
        - Reads the transformed CSV file
        - Connects to Snowflake using configured connection
        - Creates a staging table for temporary data storage
        - Loads data in batches (500 rows at a time) to staging table
        - Uses MERGE to update existing records or insert new ones
        - Uses explicit transaction control (BEGIN/COMMIT/ROLLBACK)
        - Handles NULL values and escapes special characters
        """
        logger = logging.getLogger(__name__)
        logger.info("\n" + "=" * 80)
        logger.info("[TASK: LOAD] Loading WARN data into Snowflake")
        logger.info("=" * 80 + "\n")
        
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Transformed file not found: {file_path}")
        
        # Read transformed CSV
        df = pd.read_csv(file_path)
        all_records_count = len(df)
        logger.info(f"Loading {all_records_count} rows to Snowflake")
        
        if all_records_count == 0:
            raise Exception("[LOAD] No data to load from transformed file")
        
        # Snowflake column order 
        SF_COLUMN_ORDER = [
            "STATE",
            "COMPANY",
            "CITY",
            "NUMBER_OF_WORKERS",
            "WARN_RECEIVED_DATE",
            "EFFECTIVE_DATE",
            "CLOSURE_LAYOFF",
            "LAYOFFDURATION",
            "REGION",
            "COUNTY",
            "INDUSTRY",
            "LOAD_TIMESTAMP",
        ]
        
        # DataFrame field order 
        DF_FIELD_ORDER = [
            "STATE",
            "COMPANY",
            "CITY",
            "NUMBER_OF_WORKERS",
            "WARN_RECEIVED_DATE",
            "EFFECTIVE_DATE",
            "CLOSURE_LAYOFF",
            "LAYOFFDURATION",
            "REGION",
            "COUNTY",
            "INDUSTRY",
        ]
        
        conn, db_executor = get_snowflake_executor()
        
        try:
            logger.info("[LOAD] BEGIN transaction")
            db_executor.execute("BEGIN;")
            
            # Use fully qualified table names
            staging_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{WARN_TABLE_NAME}_staging"
            main_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{WARN_TABLE_NAME}"
            
            # Create staging table
            logger.info(f"[LOAD] Creating staging table: {staging_table}")
            create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_table} (
                STATE               VARCHAR(100),
                COMPANY             VARCHAR(500),
                CITY                VARCHAR(255),
                NUMBER_OF_WORKERS   NUMBER,
                WARN_RECEIVED_DATE  TIMESTAMP_NTZ,
                EFFECTIVE_DATE      TIMESTAMP_NTZ,
                CLOSURE_LAYOFF      VARCHAR(50),
                LAYOFFDURATION      VARCHAR(50),
                REGION              VARCHAR(100),
                COUNTY              VARCHAR(100),
                INDUSTRY            VARCHAR(255),
                LOAD_TIMESTAMP      TIMESTAMP_NTZ
            )
            """
            db_executor.execute(create_staging_sql)
            logger.info("[LOAD] Staging table created")
            
            # Load data into staging table in batches
            logger.info(f"[LOAD] Loading {len(df)} rows to {staging_table}...")
            batch_size = 500
            total_rows = 0
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                
                values_clauses = []
                for row_idx, (_, row) in enumerate(batch_df.iterrows()):
                    vals = []
                    for field in DF_FIELD_ORDER:
                        val = row[field]
                        if pd.isna(val) or val is None or val == '':
                            vals.append("NULL")
                        elif field == "NUMBER_OF_WORKERS":
                           
                            try:
                                
                                num_val = str(val).replace(",", "").strip()
                                if num_val and num_val != 'nan':
                                    vals.append(num_val)
                                else:
                                    vals.append("NULL")
                            except:
                                vals.append("NULL")
                        elif isinstance(val, str):
                            escaped = val.replace("'", "''")
                            vals.append(f"'{escaped}'")
                        else:
                            vals.append(f"'{str(val)}'")
                    
                    vals.append("CURRENT_TIMESTAMP()")
                    values_clauses.append(f"({', '.join(vals)})")
                
                insert_sql = f"""
                INSERT INTO {staging_table}
                ({', '.join(SF_COLUMN_ORDER)})
                VALUES {', '.join(values_clauses)}
                """
                db_executor.execute(insert_sql)
                batch_rows = len(values_clauses)
                total_rows += batch_rows
                logger.info(
                    f"[LOAD] Batch {i//batch_size + 1}: Inserted {batch_rows} rows "
                    f"(Total: {total_rows})"
                )
            
            # Verify staging table
            staging_verify_sql = f"SELECT COUNT(*) as staging_count FROM {staging_table}"
            db_executor.execute(staging_verify_sql)
            staging_count = db_executor.fetchone()[0]
            logger.info(f"[LOAD] Staging table verification: {staging_count} rows")
            
            if staging_count == 0:
                raise Exception(
                    f"[LOAD] ERROR: Staging table {staging_table} is empty after INSERT!"
                )
            
            # MERGE staging data into main table
            logger.info("[LOAD] Merging staging data into main table...")
            merge_sql = f"""
            MERGE INTO {main_table} t
            USING (
                SELECT *
                FROM {staging_table}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY COMPANY, EFFECTIVE_DATE, STATE 
                    ORDER BY LOAD_TIMESTAMP DESC
                ) = 1
            ) s
            ON t.COMPANY = s.COMPANY 
               AND t.EFFECTIVE_DATE = s.EFFECTIVE_DATE
               AND t.STATE = s.STATE
            WHEN MATCHED THEN UPDATE SET
                t.CITY = s.CITY,
                t.NUMBER_OF_WORKERS = s.NUMBER_OF_WORKERS,
                t.WARN_RECEIVED_DATE = s.WARN_RECEIVED_DATE,
                t.CLOSURE_LAYOFF = s.CLOSURE_LAYOFF,
                t.LAYOFFDURATION = s.LAYOFFDURATION,
                t.REGION = s.REGION,
                t.COUNTY = s.COUNTY,
                t.INDUSTRY = s.INDUSTRY,
                t.LOAD_TIMESTAMP = s.LOAD_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(SF_COLUMN_ORDER)}
            ) VALUES (
                {', '.join('s.' + c for c in SF_COLUMN_ORDER)}
            )
            """
            db_executor.execute(merge_sql)
            logger.info("[LOAD] Merge complete")
            
            # Verify final load
            verify_sql = f"SELECT COUNT(*) as row_count FROM {main_table}"
            db_executor.execute(verify_sql)
            final_row_count = db_executor.fetchone()[0]
            logger.info(
                f"[LOAD] Verified: {main_table} now contains {final_row_count} rows"
            )
            
            if final_row_count == 0:
                raise Exception(
                    f"[LOAD] ERROR: Main table {main_table} is empty after MERGE!"
                )
            
            # Get load statistics
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT STATE) as unique_states,
                COUNT(DISTINCT COMPANY) as unique_companies,
                COUNT(DISTINCT INDUSTRY) as unique_industries,
                SUM(NUMBER_OF_WORKERS) as total_workers_affected,
                MIN(EFFECTIVE_DATE) as earliest_date,
                MAX(EFFECTIVE_DATE) as latest_date
            FROM {main_table}
            """
            db_executor.execute(stats_sql)
            result = db_executor.fetchone()
            
            stats = {
                'total_records': result[0],
                'unique_states': result[1],
                'unique_companies': result[2],
                'unique_industries': result[3],
                'total_workers_affected': int(result[4]) if result[4] else None,
                'earliest_date': str(result[5]) if result[5] else None,
                'latest_date': str(result[6]) if result[6] else None,
                'rows_loaded': total_rows,
                'final_row_count': final_row_count,
            }
            
            logger.info("[LOAD] COMMIT transaction")
            db_executor.execute("COMMIT;")
            
            logger.info("\n" + "=" * 80)
            logger.info("[LOAD] LOAD PHASE COMPLETE")
            logger.info(f"[LOAD] Records transformed: {all_records_count}")
            logger.info(f"[LOAD] Rows loaded: {total_rows}")
            logger.info(f"[LOAD] Final table size: {final_row_count}")
            logger.info("Load statistics:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
            logger.info("=" * 80 + "\n")
            
            return stats
            
        except Exception as e:
            logger.error("\n" + "=" * 80)
            logger.error(f"[LOAD] ERROR IN LOAD TASK: {e}")
            logger.error("[LOAD] ROLLBACK transaction")
            try:
                db_executor.execute("ROLLBACK;")
            except Exception as rb_err:
                logger.error(f"[LOAD] Error during rollback: {rb_err}")
            logger.error("=" * 80 + "\n")
            raise
        
        finally:
            db_executor.close()
            conn.close()
    
    # Task 0: Create/Update Snowflake table schema
    @task(task_id='create_snowflake_table')
    def create_table():
        """Create Snowflake table schema"""
        
        logger = logging.getLogger(__name__)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        sql_content = f"""
CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE};
CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA};
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};

CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{WARN_TABLE_NAME} (
    STATE               VARCHAR(100),
    COMPANY             VARCHAR(500),
    CITY                VARCHAR(255),
    NUMBER_OF_WORKERS   NUMBER,
    WARN_RECEIVED_DATE  TIMESTAMP_NTZ,
    EFFECTIVE_DATE      TIMESTAMP_NTZ,
    CLOSURE_LAYOFF      VARCHAR(50),
    LAYOFFDURATION      VARCHAR(50),
    REGION              VARCHAR(100),
    COUNTY              VARCHAR(100),
    INDUSTRY            VARCHAR(255),
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
        """
        
        # Split SQL by semicolon into individual statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        conn = hook.get_conn()
        executor = conn.cursor()
        
        try:
            logger.info("Beginning CREATE/USE execution")
            
            # Execute statements
            for stmt in statements:
                logger.info(f"Executing: {stmt[:120]}...")
                executor.execute(stmt)
            
            logger.info("Schema created successfully")
            
        except Exception as e:
            logger.error(f"Error during schema creation: {e}")
            raise
        
        finally:
            executor.close()
            conn.close()
        
        return True
    
    # Task 4: Data quality checks
    quality_check_task = SnowflakeOperator(
        task_id='data_quality_checks',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT STATE) as unique_states,
            COUNT(DISTINCT COMPANY) as unique_companies,
            COUNT(DISTINCT INDUSTRY) as unique_industries,
            SUM(NUMBER_OF_WORKERS) as total_workers_affected,
            MIN(EFFECTIVE_DATE) as earliest_date,
            MAX(EFFECTIVE_DATE) as latest_date
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{WARN_TABLE_NAME};
        """,
    )
    
    # Define task dependencies using TaskFlow API
    # Call each task once and chain them properly
    schema_created = create_table()
    input_data_path = extract_warn_data()
    transformed_data_path = transform_warn_data(input_data_path)
    load_stats = load_warn_to_snowflake(transformed_data_path)
    
    # Set dependencies
    schema_created >> input_data_path >> transformed_data_path >> load_stats >> quality_check_task


# Instantiate the DAG
warn_etl_dag = warn_etl_pipeline()
