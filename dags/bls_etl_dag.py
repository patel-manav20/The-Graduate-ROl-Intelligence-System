from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.bls_transformer import transform_bls_data, OUTPUT_FILE_PATH
from config.snowflake_config import SNOWFLAKE_CONN_ID, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, BLS_TABLE_NAME

default_args = {
    'owner': 'Manav Patel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bls_employment_projections_etl',
    default_args=default_args,
    description='ETL pipeline for BLS Employment Projections data',
    schedule_interval='@monthly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bls', 'employment', 'etl'],
)


def extract_and_transform(**context):
    """
    Task 1: Extract and Transform
    - Reads the original CSV file from Downloads folder
    - Transforms data by splitting occupation titles 
    - Creates separate rows for each job title
    - Saves transformed data to project directory
    """
    df = transform_bls_data()
    context['ti'].xcom_push(key='transformed_file_path', value=OUTPUT_FILE_PATH)
    context['ti'].xcom_push(key='row_count', value=len(df))
    return OUTPUT_FILE_PATH


def load_to_snowflake(**context):
    """
    Task 2: Load to Snowflake
    - Gets transformed file path from previous task
    - Connects to Snowflake using configured connection
    - Truncates existing table to avoid duplicates
    - Reads transformed CSV file
    - Loads data in batches (1000 rows at a time) to Snowflake
    - Handles NULL values and escapes special characters
    """
    ti = context['ti']
    file_path = ti.xcom_pull(key='transformed_file_path', task_ids='extract_and_transform')
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Clear existing data before loading new data
    try:
        snowflake_hook.run(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME}")
    except:
        pass
    
    # Read transformed CSV file
    df = pd.read_csv(file_path)
    records = df.to_dict('records')
    
    # Load data in batches to avoid memory issues
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        values_list = []
        
        # Prepare SQL values for each record in batch
        for record in batch:
            values = []
            for key in ['Occupation Title', 'Job Title', 'Occupation Code', 
                       'Employment 2024', 'Employment 2034', 'Employment Change, 2024-2034',
                       'Employment Percent Change, 2024-2034', 'Occupational Openings, 2024-2034 Annual Average',
                       'Median Annual Wage 2024', 'Typical Entry-Level Education', 'Education Code',
                       'Work Experience in a Related Occupation', 'Workex Code', 
                       'Typical on-the-job Training', 'trCode']:
                val = record.get(key, '')
                # Handle NULL values
                if pd.isna(val) or val == '' or val == 'N/A':
                    values.append('NULL')
                else:
                    # Escape single quotes in strings
                    val_str = str(val).replace("'", "''")
                    values.append(f"'{val_str}'")
            values_list.append(f"({', '.join(values)})")
        
        # Insert batch into Snowflake
        insert_sql = f"""
        INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BLS_TABLE_NAME}
        (OCCUPATION_TITLE, JOB_TITLE, OCCUPATION_CODE, EMPLOYMENT_2024, EMPLOYMENT_2034,
         EMPLOYMENT_CHANGE_2024_2034, EMPLOYMENT_PERCENT_CHANGE_2024_2034,
         OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG, MEDIAN_ANNUAL_WAGE_2024,
         TYPICAL_ENTRY_LEVEL_EDUCATION, EDUCATION_CODE, WORK_EXPERIENCE_IN_RELATED_OCCUPATION,
         WORKEX_CODE, TYPICAL_ON_THE_JOB_TRAINING, TR_CODE)
        VALUES {', '.join(values_list)}
        """
        snowflake_hook.run(insert_sql)
    
    return f"Loaded {len(records)} rows"


# Task 0: Create Table
create_table_task = SnowflakeOperator(
    task_id='create_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
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
    )
    """,
    dag=dag,
)

# Task 1: Extract and Transform
extract_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag,
)

# Task 2: Load to Snowflake
load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

# Pipeline flow: Create Table → Extract & Transform → Load to Snowflake
create_table_task >> extract_transform_task >> load_task
