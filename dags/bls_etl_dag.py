"""
BLS Employment Projections ETL Pipeline
=======================================
This DAG extracts, transforms, and loads BLS Employment Projections data into Snowflake.

Schedule: Monthly
Purpose: Employment projections and wage data for ROI forecasts
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.bls_transformer import transform_bls_data, OUTPUT_FILE_PATH
from config.snowflake_config import SNOWFLAKE_CONN_ID, SNOWFLAKE_DATABASE, BLS_TABLE_NAME

# Use RAW schema for BLS data (matching Adzuna pattern)
SNOWFLAKE_SCHEMA = 'RAW'

default_args = {
    'owner': 'Manav Patel',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email': [],
}


def get_snowflake_executor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    executor = conn.cursor()
    return conn, executor


@dag(
    dag_id='bls_employment_projections_etl',
    default_args=default_args,
    description='ETL pipeline for BLS Employment Projections data',
    schedule_interval='@monthly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bls', 'employment', 'etl'],
)
def bls_etl_pipeline():
    """
    Main ETL pipeline DAG using TaskFlow API
    """
    
    @task(task_id='extract_and_transform')
    def extract_and_transform() -> str:
        """
        Extract and Transform: Reads the original CSV file and transforms data.
        - Reads the original CSV file from Downloads folder
        - Transforms data by splitting occupation titles 
        - Creates separate rows for each job title
        - Saves transformed data to project directory
        
        Returns:
            Path to transformed CSV file
        """
        logger = logging.getLogger(__name__)
        logger.info("Starting BLS data extraction and transformation")
        
        df = transform_bls_data()
        logger.info(f"Transformed {len(df)} rows")
        
        return OUTPUT_FILE_PATH
    
    @task(task_id='load_to_snowflake')
    def load_to_snowflake(file_path: str) -> dict:
        """
        Load to Snowflake using staging table and MERGE.
        - Gets transformed file path from previous task
        - Connects to Snowflake using configured connection
        - Creates a staging table for temporary data storage
        - Loads data in batches (1000 rows at a time) to staging table
        - Uses MERGE to update existing records or insert new ones
        - Uses explicit transaction control (BEGIN/COMMIT/ROLLBACK)
        - Handles NULL values and escapes special characters
        """
        logger = logging.getLogger(__name__)
        logger.info("\n" + "=" * 80)
        logger.info("[TASK: LOAD] Loading BLS Employment Projections data into Snowflake")
        logger.info("=" * 80 + "\n")
        
        if not file_path:
            raise Exception("[LOAD] No transformed file path found from previous task")
        
        df = pd.read_csv(file_path)
        all_records_count = len(df)
        
        if all_records_count == 0:
            raise Exception("[LOAD] No data to load from transformed file")
        
        SF_COLUMN_ORDER = [
            "OCCUPATION_TITLE",
            "JOB_TITLE",
            "OCCUPATION_CODE",
            "EMPLOYMENT_2024",
            "EMPLOYMENT_2034",
            "EMPLOYMENT_CHANGE_2024_2034",
            "EMPLOYMENT_PERCENT_CHANGE_2024_2034",
            "OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG",
            "MEDIAN_ANNUAL_WAGE_2024",
            "TYPICAL_ENTRY_LEVEL_EDUCATION",
            "EDUCATION_CODE",
            "WORK_EXPERIENCE_IN_RELATED_OCCUPATION",
            "WORKEX_CODE",
            "TYPICAL_ON_THE_JOB_TRAINING",
            "TR_CODE",
            "LOAD_TIMESTAMP",
        ]
        
        DF_FIELD_ORDER = [
            'Occupation Title',
            'Job Title',
            'Occupation Code',
            'Employment 2024',
            'Employment 2034',
            'Employment Change, 2024-2034',
            'Employment Percent Change, 2024-2034',
            'Occupational Openings, 2024-2034 Annual Average',
            'Median Annual Wage 2024',
            'Typical Entry-Level Education',
            'Education Code',
            'Work Experience in a Related Occupation',
            'Workex Code',
            'Typical on-the-job Training',
            'trCode',
        ]
        
        conn, db_executor = get_snowflake_executor()
        
        try:
            logger.info("[LOAD] BEGIN transaction")
            db_executor.execute("BEGIN;")
            
            staging_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME}_staging"
            main_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME}"
            
            logger.info(f"[LOAD] Creating staging table: {staging_table}")
            create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_table} (
                OCCUPATION_TITLE VARCHAR(500),
                JOB_TITLE VARCHAR(500),
                OCCUPATION_CODE VARCHAR(20),
                EMPLOYMENT_2024 VARCHAR(50),
                EMPLOYMENT_2034 VARCHAR(50),
                EMPLOYMENT_CHANGE_2024_2034 VARCHAR(50),
                EMPLOYMENT_PERCENT_CHANGE_2024_2034 VARCHAR(50),
                OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG VARCHAR(50),
                MEDIAN_ANNUAL_WAGE_2024 VARCHAR(50),
                TYPICAL_ENTRY_LEVEL_EDUCATION VARCHAR(200),
                EDUCATION_CODE VARCHAR(10),
                WORK_EXPERIENCE_IN_RELATED_OCCUPATION VARCHAR(100),
                WORKEX_CODE VARCHAR(10),
                TYPICAL_ON_THE_JOB_TRAINING VARCHAR(100),
                TR_CODE VARCHAR(10),
                LOAD_TIMESTAMP TIMESTAMP_NTZ
            )
            """
            db_executor.execute(create_staging_sql)
            logger.info("[LOAD] Staging table created")
            
            logger.info(f"[LOAD] Loading {len(df)} rows to {staging_table}...")
            batch_size = 1000
            total_rows = 0
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                
                values_clauses = []
                for row_idx, (_, row) in enumerate(batch_df.iterrows()):
                    vals = []
                    for field in DF_FIELD_ORDER:
                        val = row[field]
                        if pd.isna(val) or val == '' or val == 'N/A':
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
            
            staging_verify_sql = f"SELECT COUNT(*) as staging_count FROM {staging_table}"
            db_executor.execute(staging_verify_sql)
            staging_count = db_executor.fetchone()[0]
            logger.info(f"[LOAD] Staging table verification: {staging_count} rows")
            
            if staging_count == 0:
                raise Exception(
                    f"[LOAD] ERROR: Staging table {staging_table} is empty after INSERT!"
                )
            
            logger.info("[LOAD] Merging staging data into main table...")
            merge_sql = f"""
            MERGE INTO {main_table} t
            USING {staging_table} s
            ON t.OCCUPATION_CODE = s.OCCUPATION_CODE 
               AND t.JOB_TITLE = s.JOB_TITLE
            WHEN MATCHED THEN UPDATE SET
                t.OCCUPATION_TITLE = s.OCCUPATION_TITLE,
                t.EMPLOYMENT_2024 = s.EMPLOYMENT_2024,
                t.EMPLOYMENT_2034 = s.EMPLOYMENT_2034,
                t.EMPLOYMENT_CHANGE_2024_2034 = s.EMPLOYMENT_CHANGE_2024_2034,
                t.EMPLOYMENT_PERCENT_CHANGE_2024_2034 = s.EMPLOYMENT_PERCENT_CHANGE_2024_2034,
                t.OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG = s.OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG,
                t.MEDIAN_ANNUAL_WAGE_2024 = s.MEDIAN_ANNUAL_WAGE_2024,
                t.TYPICAL_ENTRY_LEVEL_EDUCATION = s.TYPICAL_ENTRY_LEVEL_EDUCATION,
                t.EDUCATION_CODE = s.EDUCATION_CODE,
                t.WORK_EXPERIENCE_IN_RELATED_OCCUPATION = s.WORK_EXPERIENCE_IN_RELATED_OCCUPATION,
                t.WORKEX_CODE = s.WORKEX_CODE,
                t.TYPICAL_ON_THE_JOB_TRAINING = s.TYPICAL_ON_THE_JOB_TRAINING,
                t.TR_CODE = s.TR_CODE,
                t.LOAD_TIMESTAMP = s.LOAD_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(SF_COLUMN_ORDER)}
            ) VALUES (
                {', '.join('s.' + c for c in SF_COLUMN_ORDER)}
            )
            """
            db_executor.execute(merge_sql)
            logger.info("[LOAD] Merge complete")
            
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
            
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT OCCUPATION_CODE) as unique_occupations,
                COUNT(DISTINCT JOB_TITLE) as unique_job_titles,
                MIN(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as min_wage,
                MAX(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as max_wage,
                AVG(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as avg_wage
            FROM {main_table}
            WHERE MEDIAN_ANNUAL_WAGE_2024 IS NOT NULL 
              AND MEDIAN_ANNUAL_WAGE_2024 != ''
              AND MEDIAN_ANNUAL_WAGE_2024 != 'N/A'
            """
            db_executor.execute(stats_sql)
            result = db_executor.fetchone()
            
            stats = {
                'total_records': result[0],
                'unique_occupations': result[1],
                'unique_job_titles': result[2],
                'min_wage': str(result[3]) if result[3] else None,
                'max_wage': str(result[4]) if result[4] else None,
                'avg_wage': float(result[5]) if result[5] else None,
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

CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME} (
    OCCUPATION_TITLE VARCHAR(500),
    JOB_TITLE VARCHAR(500),
    OCCUPATION_CODE VARCHAR(20),
    EMPLOYMENT_2024 VARCHAR(50),
    EMPLOYMENT_2034 VARCHAR(50),
    EMPLOYMENT_CHANGE_2024_2034 VARCHAR(50),
    EMPLOYMENT_PERCENT_CHANGE_2024_2034 VARCHAR(50),
    OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG VARCHAR(50),
    MEDIAN_ANNUAL_WAGE_2024 VARCHAR(50),
    TYPICAL_ENTRY_LEVEL_EDUCATION VARCHAR(200),
    EDUCATION_CODE VARCHAR(10),
    WORK_EXPERIENCE_IN_RELATED_OCCUPATION VARCHAR(100),
    WORKEX_CODE VARCHAR(10),
    TYPICAL_ON_THE_JOB_TRAINING VARCHAR(100),
    TR_CODE VARCHAR(10),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
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
            COUNT(DISTINCT OCCUPATION_CODE) as unique_occupations,
            COUNT(DISTINCT JOB_TITLE) as unique_job_titles,
            MIN(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as min_wage,
            MAX(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as max_wage,
            AVG(TRY_CAST(REPLACE(MEDIAN_ANNUAL_WAGE_2024, ',', '') AS FLOAT)) as avg_wage
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME}
        WHERE MEDIAN_ANNUAL_WAGE_2024 IS NOT NULL 
          AND MEDIAN_ANNUAL_WAGE_2024 != ''
          AND MEDIAN_ANNUAL_WAGE_2024 != 'N/A';
        """,
    )
    
    # Call each task once and chain them properly
    schema_created = create_table()
    transformed_data_path = extract_and_transform()
    load_stats = load_to_snowflake(transformed_data_path)
    
    # Set dependencies
    schema_created >> transformed_data_path >> load_stats >> quality_check_task


# Instantiate the DAG
bls_etl_dag = bls_etl_pipeline()
