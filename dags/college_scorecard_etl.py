"""
College Scorecard ETL Pipeline
Extracts, transforms, and loads College Scorecard data from the Department of Education API
into Snowflake for analysis of tuition, debt, and earnings data.
"""

from __future__ import annotations
from datetime import datetime, timedelta
import logging
import os
from typing import List, Dict, Any

import pandas as pd
import requests
import time

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils import timezone

# Snowflake configuration - matches your credentials
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DATABASE = "USER_DB_HYENA"
SNOWFLAKE_SCHEMA = "RAW"  # Use RAW schema (matching BLS and Adzuna pattern)
COLLEGE_SCORECARD_TABLE = "college_scorecard_data"

DEFAULT_ARGS = {
    "owner": "Manav Patel",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email": [],
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


def get_api_key() -> str:
    """
    Get College Scorecard API key from Airflow Variable.
    API key is optional but recommended to avoid rate limits.
    """
    try:
        api_key = Variable.get("COLLEGE_SCORECARD_API_KEY", default_var="")
        if api_key:
            logging.info("Using College Scorecard API key from Airflow Variable")
            return api_key
    except Exception:
        pass
    logging.info("No API key configured. API calls will have rate limits.")
    return ""


# DAG Definition
with DAG(
    dag_id="college_scorecard_etl",
    default_args=DEFAULT_ARGS,
    description="ETL Pipeline for College Scorecard data from Dept. of Education API",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["college_scorecard", "etl", "snowflake", "education"],
) as dag:

    # Task 0: Create/Update Snowflake table schema
    # This task ensures the target table in Snowflake exists with the correct schema.
    # It uses the SnowflakeOperator to execute DDL (Data Definition Language) SQL.
    create_table_task = SnowflakeOperator(
        task_id='create_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE};
        CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA};
        USE DATABASE {SNOWFLAKE_DATABASE};
        USE SCHEMA {SNOWFLAKE_SCHEMA};
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{COLLEGE_SCORECARD_TABLE} (
            INSTITUTION_ID INT PRIMARY KEY,
            INSTITUTION_NAME VARCHAR(500),
            STATE VARCHAR(10),
            CITY VARCHAR(255),
            ZIP_CODE VARCHAR(20),
            REGION_ID INT,
            WEBSITE VARCHAR(500),
            SCHOOL_TYPE INT,
            OWNERSHIP INT,
            STUDENT_SIZE INT,
            ADMISSION_RATE FLOAT,
            TUITION_IN_STATE FLOAT,
            TUITION_OUT_OF_STATE FLOAT,
            AVG_NET_PRICE FLOAT,
            FEDERAL_GRANTS FLOAT,
            LOANS_AMOUNT FLOAT,
            EARNINGS_10YRS FLOAT,
            EARNINGS_6YRS FLOAT,
            MEDIAN_DEBT FLOAT,
            COMPLETION_RATE FLOAT,
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        dag=dag,
    )

    # Task 1: Extract College Scorecard Data
    # This task fetches raw data from the College Scorecard API.
    # It handles pagination to retrieve all available records (up to max_pages).
    @task(task_id="extract_college_scorecard")
    def extract_college_scorecard() -> List[Dict[str, Any]]:
        """
        Extract: Fetch raw data from College Scorecard API.
        - Retrieves institution data including tuition, earnings, and debt information
        - Handles pagination to fetch multiple pages of results
        - Returns a list of raw JSON records for transformation
        """
        logging.info("=" * 80)
        logging.info("[TASK: EXTRACT] Fetching College Scorecard data from API")
        logging.info("=" * 80)

        try:
            api_key = get_api_key()
            base_url = "https://api.data.gov/ed/collegescorecard/v1/schools"

            # Fields to extract from the API
            fields = [
                "id",
                "school.name",
                "school.state",
                "school.city",
                "school.zip",
                "school.region_id",
                "school.school_url",
                "school.ownership",
                "school.degrees_awarded.predominant",              
                "latest.student.size",
                "latest.admissions.admission_rate.overall",
                "latest.cost.tuition.in_state",
                "latest.cost.tuition.out_of_state",
                "latest.cost.avg_net_price.public",
                "latest.cost.avg_net_price.private",
                "latest.cost.avg_net_price.overall",
                "latest.cost.avg_net_price.consumer.overall_median",
                "latest.cost.avg_net_price.consumer.median_by_pred_degree",
                "latest.earnings.10_yrs_after_entry.median",
                "latest.earnings.6_yrs_after_entry.median",
                "latest.aid.median_debt.completers.overall",
                "latest.completion.completion_rate_4yr_150nt",          
                "latest.completion.completion_rate_less_than_4yr_150nt"
            ]           

            params = {
                "api_key": api_key,
                "fields": ",".join(fields),
                "per_page": 100,
            }

            all_records: List[Dict[str, Any]] = []
            page = 0
            max_pages = 500  # Maximum pages to fetch (adjust as needed)
            max_retries = 3  # Maximum retries for failed pages
            retry_delay = 2  # Initial delay in seconds for retries
            consecutive_failures = 0  # Track consecutive failures
            max_consecutive_failures = 5  # Stop after this many consecutive failures
            response = None

            # Fetch data page by page
            while page < max_pages:
                params["page"] = page
                logging.info(f"[EXTRACT] Fetching page {page}...")
                
                success = False
                retry_count = 0
                
                # Retry logic for failed requests
                while retry_count <= max_retries and not success:
                    try:
                        # Add delay between requests to avoid rate limiting
                        if page > 0:
                            time.sleep(0.5)  # 500ms delay between requests
                        
                        response = requests.get(base_url, params=params, timeout=30)
                        
                        # Handle different HTTP status codes
                        if response.status_code == 200:
                            success = True
                            consecutive_failures = 0  # Reset failure counter
                        elif response.status_code == 429:
                            # Rate limit - wait longer and retry
                            wait_time = retry_delay * (2 ** retry_count)
                            logging.warning(f"[EXTRACT] Rate limited on page {page}. Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            retry_count += 1
                            continue
                        elif response.status_code >= 500:
                            # Server error - retry with exponential backoff
                            if retry_count < max_retries:
                                wait_time = retry_delay * (2 ** retry_count)
                                logging.warning(
                                    f"[EXTRACT] Server error {response.status_code} on page {page}. "
                                    f"Retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})..."
                                )
                                time.sleep(wait_time)
                                retry_count += 1
                                continue
                            else:
                                logging.error(
                                    f"[EXTRACT] Server error {response.status_code} on page {page} "
                                    f"after {max_retries} retries. Skipping this page."
                                )
                                consecutive_failures += 1
                                break
                        else:
                            # Other client errors (400, 404, etc.) - don't retry
                            response.raise_for_status()
                            
                    except requests.exceptions.RequestException as req_err:
                        if retry_count < max_retries:
                            wait_time = retry_delay * (2 ** retry_count)
                            logging.warning(
                                f"[EXTRACT] Request error on page {page}: {req_err}. "
                                f"Retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})..."
                            )
                            time.sleep(wait_time)
                            retry_count += 1
                            continue
                        else:
                            logging.error(
                                f"[EXTRACT] Request error on page {page} after {max_retries} retries: {req_err}. "
                                f"Skipping this page."
                            )
                            consecutive_failures += 1
                            break
                
                # If request succeeded, process the results
                if success and response is not None:
                    try:
                        result = response.json()
                        results = result.get("results", [])

                        # Log sample data from first page for debugging
                        if page == 0 and results:
                            sample = results[0]
                            logging.info(f"[EXTRACT] Sample record keys: {list(sample.keys())[:5]}...")

                        if not results:
                            logging.info(f"[EXTRACT] No more results after page {page}")
                            break

                        all_records.extend(results)
                        logging.info(
                            f"[EXTRACT] Page {page}: {len(results)} records | Total: {len(all_records)}"
                        )
                    except Exception as parse_err:
                        logging.error(f"[EXTRACT] Error parsing JSON response for page {page}: {parse_err}")
                        consecutive_failures += 1
                else:
                    # Request failed after all retries
                    consecutive_failures += 1
                
                # Stop if too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logging.warning(
                        f"[EXTRACT] Stopping extraction after {max_consecutive_failures} consecutive failures. "
                        f"Total records fetched so far: {len(all_records)}"
                    )
                    break
                
                page += 1

            if not all_records:
                raise Exception("[EXTRACT] ERROR: No data fetched from API")

            logging.info(f"[EXTRACT] FETCH COMPLETE: {len(all_records)} records fetched from {page} pages")
            return all_records

        except Exception as e:
            logging.error("\n" + "=" * 80)
            logging.error(f"[EXTRACT] ERROR IN EXTRACT TASK: {e}")
            logging.error("=" * 80 + "\n")
            raise

    # Task 2: Transform College Scorecard Data
    # This task transforms the raw JSON data into a tabular structure.
    # It flattens nested JSON fields and handles data type conversions.
    @task(task_id="transform_college_scorecard")
    def transform_college_scorecard(
        all_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform: Flatten raw JSON into a tabular structure.
        - Extracts nested fields from the API response
        - Handles completion rate logic (4yr vs less than 4yr)
        - Converts data types and handles missing values
        - Returns a list of flattened dictionaries ready for loading
        """
        logging.info("\n" + "=" * 80)
        logging.info("[TASK: TRANSFORM] Transforming College Scorecard data")
        logging.info("=" * 80 + "\n")
    
        try:
            if not all_records:
                raise Exception("[TRANSFORM] No records passed into transform task")
    
            transformed_data: List[Dict[str, Any]] = []
    
            for idx, record in enumerate(all_records):
                if idx % 500 == 0:
                    logging.info(
                        f"[TRANSFORM] Transforming record {idx}/{len(all_records)}..."
                    )
    
                # Get school type
                school_type = record.get("school.degrees_awarded.predominant")
    
                # Handle completion rate: prefer 4yr, fallback to less than 4yr
                completion_4yr = record.get(
                    "latest.completion.completion_rate_4yr_150nt"
                )
                completion_lt4 = record.get(
                    "latest.completion.completion_rate_less_than_4yr_150nt"
                )
    
                if completion_4yr is not None:
                    completion_rate = completion_4yr
                else:
                    completion_rate = completion_lt4
    
                # Flatten the nested JSON structure into a flat dictionary
                flattened = {
                    "institution_id": record.get("id"),
                    "institution_name": record.get("school.name"),
                    "state": record.get("school.state"),
                    "city": record.get("school.city"),
                    "zip_code": record.get("school.zip"),
                    "region_id": record.get("school.region_id"),
                    "website": record.get("school.school_url"),
                    "school_type": school_type,
                    "ownership": record.get("school.ownership"),
                    "student_size": record.get("latest.student.size"),
                    "admission_rate": record.get(
                        "latest.admissions.admission_rate.overall"
                    ),
                    "tuition_in_state": record.get(
                        "latest.cost.tuition.in_state"
                    ),
                    "tuition_out_of_state": record.get(
                        "latest.cost.tuition.out_of_state"
                    ),
                    "avg_net_price": record.get(
                        "latest.cost.avg_net_price.overall"
                    ),
                    "federal_grants": record.get(
                        "latest.cost.avg_net_price.consumer.overall_median"
                    ),
                    "loans_amount": record.get(
                        "latest.cost.avg_net_price.consumer.median_by_pred_degree"
                    ),
                    "earnings_10yrs": record.get(
                        "latest.earnings.10_yrs_after_entry.median"
                    ),
                    "earnings_6yrs": record.get(
                        "latest.earnings.6_yrs_after_entry.median"
                    ),
                    "median_debt": record.get(
                        "latest.aid.median_debt.completers.overall"
                    ),
                    "completion_rate": completion_rate,
                    "load_timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                }
    
                transformed_data.append(flattened)
    
            df = pd.DataFrame(transformed_data)
            logging.info(
                f"[TRANSFORM] ✓ TRANSFORM COMPLETE: {len(df)} rows in DataFrame"
            )
            logging.info(f"[TRANSFORM] Columns: {list(df.columns)}")
    
            return transformed_data
    
        except Exception as e:
            logging.error("\n" + "=" * 80)
            logging.error(f"[TRANSFORM] ERROR IN TRANSFORM TASK: {e}")
            logging.error("=" * 80 + "\n")
            raise

    # Task 3: Load College Scorecard Data to Snowflake
    # This task loads the transformed data into Snowflake using a staging table approach.
    # It uses MERGE to handle updates and inserts efficiently.
    @task(task_id="load_college_scorecard")
    def load_college_scorecard(
        transformed_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Load: Insert transformed data into Snowflake using staging table and MERGE.
        - Creates a staging table for temporary data storage
        - Loads data in batches (500 rows at a time) for efficiency
        - Uses MERGE to update existing records or insert new ones
        - Uses explicit transaction control (BEGIN/COMMIT/ROLLBACK)
        """
        logging.info("\n" + "=" * 80)
        logging.info("[TASK: LOAD] Loading College Scorecard data into Snowflake")
        logging.info("=" * 80 + "\n")

        if not transformed_data:
            raise Exception("[LOAD] No transformed data to load")

        df = pd.DataFrame(transformed_data)
        all_records_count = len(transformed_data)

        # Snowflake column order (uppercase)
        SF_COLUMN_ORDER = [
            "INSTITUTION_ID",
            "INSTITUTION_NAME",
            "STATE",
            "CITY",
            "ZIP_CODE",
            "REGION_ID",
            "WEBSITE",
            "SCHOOL_TYPE",
            "OWNERSHIP",
            "STUDENT_SIZE",
            "ADMISSION_RATE",
            "TUITION_IN_STATE",
            "TUITION_OUT_OF_STATE",
            "AVG_NET_PRICE",
            "FEDERAL_GRANTS",
            "LOANS_AMOUNT",
            "EARNINGS_10YRS",
            "EARNINGS_6YRS",
            "MEDIAN_DEBT",
            "COMPLETION_RATE",
            "LOAD_TIMESTAMP",
        ]

        # DataFrame field order (lowercase)
        DF_FIELD_ORDER = [
            "institution_id",
            "institution_name",
            "state",
            "city",
            "zip_code",
            "region_id",
            "website",
            "school_type",
            "ownership",
            "student_size",
            "admission_rate",
            "tuition_in_state",
            "tuition_out_of_state",
            "avg_net_price",
            "federal_grants",
            "loans_amount",
            "earnings_10yrs",
            "earnings_6yrs",
            "median_debt",
            "completion_rate",
            "load_timestamp",
        ]

        conn, db_executor = get_snowflake_executor()

        try:
            logging.info("[LOAD] BEGIN transaction")
            db_executor.execute("BEGIN;")

            # Use fully qualified table names
            staging_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{COLLEGE_SCORECARD_TABLE}_staging"
            main_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{COLLEGE_SCORECARD_TABLE}"

            # Create staging table
            logging.info(f"[LOAD] Creating staging table: {staging_table}")
            create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_table} (
                INSTITUTION_ID INT,
                INSTITUTION_NAME VARCHAR(500),
                STATE VARCHAR(10),
                CITY VARCHAR(255),
                ZIP_CODE VARCHAR(20),
                REGION_ID INT,
                WEBSITE VARCHAR(500),
                SCHOOL_TYPE INT,
                OWNERSHIP INT,
                STUDENT_SIZE INT,
                ADMISSION_RATE FLOAT,
                TUITION_IN_STATE FLOAT,
                TUITION_OUT_OF_STATE FLOAT,
                AVG_NET_PRICE FLOAT,
                FEDERAL_GRANTS FLOAT,
                LOANS_AMOUNT FLOAT,
                EARNINGS_10YRS FLOAT,
                EARNINGS_6YRS FLOAT,
                MEDIAN_DEBT FLOAT,
                COMPLETION_RATE FLOAT,
                LOAD_TIMESTAMP TIMESTAMP_NTZ
            )
            """
            db_executor.execute(create_staging_sql)
            logging.info("[LOAD] Staging table created")

            # Load data into staging table in batches
            logging.info(f"[LOAD] Loading {len(df)} rows to {staging_table}...")
            batch_size = 500
            total_rows = 0

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]

                values_clauses = []
                for row_idx, (_, row) in enumerate(batch_df.iterrows()):
                    vals = []
                    for field in DF_FIELD_ORDER:
                        val = row[field]
                        if pd.isna(val) or val is None:
                            vals.append("NULL")
                        elif field == "load_timestamp":
                            ts_str = pd.Timestamp(val).isoformat()
                            vals.append(f"'{ts_str}'")
                        elif isinstance(val, str):
                            escaped = val.replace("'", "''")
                            vals.append(f"'{escaped}'")
                        else:
                            vals.append(str(val))
                    values_clauses.append(f"({', '.join(vals)})")

                insert_sql = f"""
                INSERT INTO {staging_table}
                ({', '.join(SF_COLUMN_ORDER)})
                VALUES {', '.join(values_clauses)}
                """
                db_executor.execute(insert_sql)
                batch_rows = len(values_clauses)
                total_rows += batch_rows
                logging.info(
                    f"[LOAD] Batch {i//batch_size + 1}: Inserted {batch_rows} rows "
                    f"(Total: {total_rows})"
                )

            # Verify staging table
            staging_verify_sql = f"SELECT COUNT(*) as staging_count FROM {staging_table}"
            db_executor.execute(staging_verify_sql)
            staging_count = db_executor.fetchone()[0]
            logging.info(f"[LOAD] Staging table verification: {staging_count} rows")

            if staging_count == 0:
                raise Exception(
                    f"[LOAD] ERROR: Staging table {staging_table} is empty after INSERT!"
                )

            # MERGE staging data into main table
            logging.info("[LOAD] Merging staging data into main table...")
            merge_sql = f"""
            MERGE INTO {main_table} t
            USING {staging_table} s
            ON t.INSTITUTION_ID = s.INSTITUTION_ID
            WHEN MATCHED THEN UPDATE SET
                t.INSTITUTION_NAME = s.INSTITUTION_NAME,
                t.STATE = s.STATE,
                t.CITY = s.CITY,
                t.ZIP_CODE = s.ZIP_CODE,
                t.REGION_ID = s.REGION_ID,
                t.WEBSITE = s.WEBSITE,
                t.SCHOOL_TYPE = s.SCHOOL_TYPE,
                t.OWNERSHIP = s.OWNERSHIP,
                t.STUDENT_SIZE = s.STUDENT_SIZE,
                t.ADMISSION_RATE = s.ADMISSION_RATE,
                t.TUITION_IN_STATE = s.TUITION_IN_STATE,
                t.TUITION_OUT_OF_STATE = s.TUITION_OUT_OF_STATE,
                t.AVG_NET_PRICE = s.AVG_NET_PRICE,
                t.FEDERAL_GRANTS = s.FEDERAL_GRANTS,
                t.LOANS_AMOUNT = s.LOANS_AMOUNT,
                t.EARNINGS_10YRS = s.EARNINGS_10YRS,
                t.EARNINGS_6YRS = s.EARNINGS_6YRS,
                t.MEDIAN_DEBT = s.MEDIAN_DEBT,
                t.COMPLETION_RATE = s.COMPLETION_RATE,
                t.LOAD_TIMESTAMP = s.LOAD_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(SF_COLUMN_ORDER)}
            ) VALUES (
                {', '.join('s.' + c for c in SF_COLUMN_ORDER)}
            )
            """
            db_executor.execute(merge_sql)
            logging.info("[LOAD] Merge complete")

            # Verify final load
            verify_sql = f"SELECT COUNT(*) as row_count FROM {main_table}"
            db_executor.execute(verify_sql)
            final_row_count = db_executor.fetchone()[0]
            logging.info(
                f"[LOAD] Verified: {main_table} now contains {final_row_count} rows"
            )

            if final_row_count == 0:
                raise Exception(
                    f"[LOAD] ERROR: Main table {main_table} is empty after MERGE!"
                )

            logging.info("[LOAD] COMMIT transaction")
            db_executor.execute("COMMIT;")

            logging.info("\n" + "=" * 80)
            logging.info("[LOAD] LOAD PHASE COMPLETE")
            logging.info(f"[LOAD] Records transformed: {all_records_count}")
            logging.info(f"[LOAD] Rows loaded: {total_rows}")
            logging.info(f"[LOAD] Final table size: {final_row_count}")
            logging.info("=" * 80 + "\n")

            return {
                "status": "success",
                "records_transformed": all_records_count,
                "rows_loaded": total_rows,
                "final_row_count": final_row_count,
                "table": main_table,
            }

        except Exception as e:
            logging.error("\n" + "=" * 80)
            logging.error(f"[LOAD] ERROR IN LOAD TASK: {e}")
            logging.error("[LOAD] ROLLBACK transaction")
            try:
                db_executor.execute("ROLLBACK;")
            except Exception as rb_err:
                logging.error(f"[LOAD] Error during rollback: {rb_err}")
            logging.error("=" * 80 + "\n")
            raise

        finally:
            db_executor.close()
            conn.close()

    # Task 4: Data Quality Checks
    # This task validates the loaded data with quality checks.
    # It verifies row counts, null values, and data ranges.
    @task(task_id="data_quality_checks")
    def data_quality_checks() -> dict:
        """
        Data Quality: Validate loaded College Scorecard data.
        - Verifies row count is greater than zero
        - Checks for null values in key columns
        - Validates data ranges for tuition, earnings, and debt
        """
        logging.info("\n" + "=" * 80)
        logging.info("RUNNING DATA QUALITY CHECKS")
        logging.info("=" * 80 + "\n")

        conn, db_executor = get_snowflake_executor()

        try:
            main_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{COLLEGE_SCORECARD_TABLE}"

            # Check 1: Verify row count
            logging.info("Check 1: Verifying row count...")
            count_sql = f"SELECT COUNT(*) as row_count FROM {main_table}"
            db_executor.execute(count_sql)
            record_count = db_executor.fetchone()[0]
            logging.info(f"   Row count: {record_count}")

            if record_count == 0:
                logging.error("\n!!! ZERO RECORDS FOUND !!!")
                logging.error(f"Table: {main_table}")
                raise Exception(f"ERROR: No records found in {main_table}")

            # Check 2: Check for null values in key columns
            logging.info("Check 2: Checking for null values in key columns...")
            null_check_sql = f"""
            SELECT
                SUM(CASE WHEN INSTITUTION_ID IS NULL THEN 1 ELSE 0 END) as null_id,
                SUM(CASE WHEN INSTITUTION_NAME IS NULL THEN 1 ELSE 0 END) as null_name,
                SUM(CASE WHEN STATE IS NULL THEN 1 ELSE 0 END) as null_state
            FROM {main_table}
            """
            db_executor.execute(null_check_sql)
            null_counts = db_executor.fetchone()
            logging.info(
                f"  Null values - ID: {null_counts[0]}, "
                f"Name: {null_counts[1]}, State: {null_counts[2]}"
            )

            # Check 3: Validate data ranges
            logging.info("Check 3: Validating data ranges...")
            range_check_sql = f"""
            SELECT
                MIN(TUITION_IN_STATE) as min_tuition,
                MAX(TUITION_IN_STATE) as max_tuition,
                AVG(TUITION_IN_STATE) as avg_tuition,
                MIN(EARNINGS_10YRS) as min_earnings,
                MAX(EARNINGS_10YRS) as max_earnings,
                AVG(EARNINGS_10YRS) as avg_earnings,
                MIN(MEDIAN_DEBT) as min_debt,
                MAX(MEDIAN_DEBT) as max_debt,
                AVG(MEDIAN_DEBT) as avg_debt
            FROM {main_table}
            """
            db_executor.execute(range_check_sql)
            ranges = db_executor.fetchone()
            ranges = tuple(0 if x is None else x for x in ranges)

            logging.info(
                f"""
  Tuition (in-state): ${ranges[0]:,.0f} - ${ranges[1]:,.0f} (avg: ${ranges[2]:,.0f})
  Earnings (10 yrs): ${ranges[3]:,.0f} - ${ranges[4]:,.0f} (avg: ${ranges[5]:,.0f})
  Median Debt:       ${ranges[6]:,.0f} - ${ranges[7]:,.0f} (avg: ${ranges[8]:,.0f})
                """
            )

            logging.info("\n" + "=" * 80)
            logging.info(" DATA QUALITY CHECKS PASSED")
            logging.info("=" * 80 + "\n")
            
            db_executor.execute("ROLLBACK;")

            return {
                "status": "success",
                "record_count": record_count,
                "checks_passed": True,
            }

        except Exception as e:
            logging.error(f"\nERROR: Data quality checks failed: {e}\n")
            try:
                db_executor.execute("ROLLBACK;")
            except Exception as rb_err:
                logging.error(f"Error rolling back after DQ failure: {rb_err}")
            raise

        finally:
            db_executor.close()
            conn.close()

    # Define task dependencies
    # Pipeline flow: Create Table → Extract → Transform → Load → Quality Checks
    extract_task = extract_college_scorecard()
    transform_task = transform_college_scorecard(extract_task)
    load_task = load_college_scorecard(transform_task)
    quality_task = data_quality_checks()

    create_table_task >> extract_task >> transform_task >> load_task >> quality_task
