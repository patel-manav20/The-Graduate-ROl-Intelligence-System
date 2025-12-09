"""
Adzuna Job Data Loading Module
===============================
Loads transformed job listing data into Snowflake data warehouse.

Uses Airflow's Snowflake connection for secure credential management.
Implements batch loading with error handling and data validation.
"""

import logging
from datetime import datetime
from typing import Optional

import math
import numpy as np
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONN_ID = "snowflake_conn"
DEFAULT_SCHEMA = "RAW"
DEFAULT_DB: Optional[str] = "USER_DB_HYENA"


class SnowflakeLoader:
    """Loader for uploading job data to Snowflake"""

    def __init__(self, conn_id: str = "snowflake_conn"):
        """
        Initialize Snowflake loader

        Args:
            conn_id: Airflow connection ID for Snowflake
        """
        self.conn_id = conn_id
        self.hook = SnowflakeHook(snowflake_conn_id=conn_id)

    def get_table_name(self) -> str:
        """Get the target table name with fully qualified name"""
        return f"{DEFAULT_DB}.{DEFAULT_SCHEMA}.job_listings"

    def get_staging_table_name(self, execution_date: str) -> str:
        """
        Get staging table name for this execution

        Args:
            execution_date: Airflow execution date (YYYY-MM-DD)

        Returns:
            Staging table name
        """
        # Replace hyphens with underscores for valid table name
        date_suffix = execution_date.replace("-", "_")
        return f"job_listings_staging_{date_suffix}"

    def create_staging_table(self, staging_table: str) -> None:
        """
        Create staging table for batch load

        Args:
            staging_table: Name of staging table
        """
        # cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_SCHEMA}")
        # cur.execute(f"USE SCHEMA {DEFAULT_SCHEMA}")
        create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {DEFAULT_SCHEMA};
        USE SCHEMA {DEFAULT_SCHEMA};
        CREATE OR REPLACE TABLE {DEFAULT_DB}.{DEFAULT_SCHEMA}.{staging_table} (
            job_id VARCHAR(255),
            job_title VARCHAR(500),
            company VARCHAR(500),
            salary_min FLOAT,
            salary_max FLOAT,
            salary_avg FLOAT,
            description TEXT,
            posting_date DATE,
            location VARCHAR(500),
            city VARCHAR(255),
            state VARCHAR(100),
            category VARCHAR(255),
            contract_type VARCHAR(100),
            contract_time VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            redirect_url TEXT,
            load_date DATE,
            extracted_at TIMESTAMP
        );
        """

        logger.info(f"Creating staging table: {staging_table}")
        # Execute DDL statements using a raw cursor (no hook.run)
        conn = self.hook.get_conn()
        cur = conn.cursor()
        try:
            # split into individual statements and execute
            statements = [s.strip() for s in create_sql.split(';') if s.strip()]
            logger.debug(f"Executing DDL statements: {statements}")
            for stmt in statements:
                cur.execute(stmt)
            conn.commit()
            logger.info(f"Staging table {staging_table} created/updated via cursor.execute")
        finally:
            cur.close()

    @staticmethod
    def _clean_value(v):
        """Normalize NaN-like values to None for safe insertion to Snowflake."""
        if v is None:
            return None

        # numeric NaN (float, numpy floating)
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except Exception:
            pass

        try:
            if isinstance(v, (np.floating,)) and np.isnan(v):
                return None
        except Exception:
            pass

        # String variants of null / nan
        if isinstance(v, str):
            if v.strip().lower() in {"nan", "none", "null", "na", "n/a", "nan.0"}:
                return None
            return v

        return v

    def load_csv_to_staging(self, csv_path: str, staging_table: str) -> int:
        """
        Load CSV data into staging table

        Args:
            csv_path: Path to CSV file
            staging_table: Name of staging table

        Returns:
            Number of rows loaded
        """
        logger.info(f"Loading data from {csv_path} to {staging_table}")

        # Read CSV
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} rows from CSV")
        logger.info(f"Original columns from CSV: {list(df.columns)}")

        # --- Normalize column names and enforce schema to avoid 'NAN' identifier issues ---

        # Make sure all column names are strings and strip whitespace
        df.columns = [str(c).strip() for c in df.columns]
        logger.info(f"Normalized columns from CSV: {list(df.columns)}")

        expected_columns = [
            "job_id",
            "job_title",
            "company",
            "salary_min",
            "salary_max",
            "salary_avg",
            "description",
            "posting_date",
            "location",
            "city",
            "state",
            "category",
            "contract_type",
            "contract_time",
            "latitude",
            "longitude",
            "redirect_url",
            "load_date",
            "extracted_at",
        ]

        # Drop unexpected/extra columns (including any weird 'nan' column)
        extra_cols = [c for c in df.columns if c not in expected_columns]
        if extra_cols:
            logger.warning(f"Dropping unexpected columns from CSV: {extra_cols}")
            df = df[[c for c in df.columns if c in expected_columns]]

        # Add any missing expected columns as None
        missing_cols = [c for c in expected_columns if c not in df.columns]
        if missing_cols:
            logger.warning(f"Adding missing columns with NULLs: {missing_cols}")
            for c in missing_cols:
                df[c] = None

        # Reorder columns to exactly match staging table definition
        df = df[expected_columns]

        logger.info(f"Normalized columns used for load: {list(df.columns)}")
        print(df.info())

        # --- Robust NaN-like cleanup before insertion into Snowflake ---

        # Ensure object dtype so we can place None safely
        try:
            df = df.astype(object)
        except Exception:
            # if astype fails, continue with existing dtypes
            pass

        # Replace pandas/numpy NaN with Python None (works across dtypes)
        try:
            df = df.where(pd.notnull(df), None)
        except Exception:
            # Fallback: apply per-cell replacement
            df = df.applymap(lambda x: None if (isinstance(x, float) and math.isnan(x)) else x)

        # Normalize string variants like 'nan', 'None', 'NULL', etc. to None
        try:
            df = df.applymap(self._clean_value)
        except Exception:
            # Last-resort: iterate columns
            for col in df.columns:
                df[col] = df[col].apply(self._clean_value)

        # Save cleaned CSV for debugging / inspection
        try:
            cleaned_path = f"/tmp/cleaned_{staging_table}.csv"
            df.to_csv(cleaned_path, index=False)
            logger.info(f"Saved cleaned CSV to {cleaned_path}")
        except Exception as e:
            logger.warning(f"Failed to save cleaned CSV: {e}")

        # Log a small sample of the cleaned DataFrame before starting DB transaction
        try:
            # to_string ensures the DataFrame sample is readable in logs
            logger.info("DataFrame head before DB transaction:\n%s", df.head().to_string())
        except Exception as e:
            logger.warning(f"Failed to log DataFrame head: {e}")

        # Convert DataFrame to list of tuples
        records = df.to_records(index=False)
        data = [tuple(record) for record in records]

        # Final defensive sanitization: ensure no NaN-like or invalid tokens remain
        sanitized_data = []
        for row in data:
            new_row = []
            for v in row:
                # Convert numpy types and floats that are NaN to None
                try:
                    if isinstance(v, float) and math.isnan(v):
                        new_row.append(None)
                        continue
                except Exception:
                    pass

                try:
                    if isinstance(v, (np.floating,)) and np.isnan(v):
                        new_row.append(None)
                        continue
                except Exception:
                    pass

                # Normalize string variants
                if isinstance(v, str) and v.strip().lower() in {
                    "nan",
                    "none",
                    "null",
                    "na",
                    "n/a",
                    "nan.0",
                }:
                    new_row.append(None)
                    continue

                new_row.append(v)

            sanitized_data.append(tuple(new_row))

        # Replace data with sanitized_data for insertion
        data = sanitized_data

        # Log first sanitized row for debugging
        if data:
            logger.debug(f"First sanitized row sample: {data[0]}")

        if not data:
            logger.info("No rows to insert into staging table (DataFrame is empty).")
            return 0

        # Build column list and placeholders
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        insert_sql = f"""
        INSERT INTO {DEFAULT_DB}.{DEFAULT_SCHEMA}.{staging_table} ({columns})
        VALUES ({placeholders})
        """

        # Use hook to insert data in batches
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        try:
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                logger.info(
                    f"Inserted batch {i // batch_size + 1}: "
                    f"{total_inserted}/{len(data)} rows"
                )

            conn.commit()
            logger.info(
                f"Successfully loaded {total_inserted} rows to staging table {staging_table}"
            )
            return total_inserted

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data: {str(e)}")
            raise
        finally:
            cursor.close()

    def merge_staging_to_target(self, staging_table: str, target_table: str) -> None:
        """
        Merge data from staging to target table (upsert)

        Args:
            staging_table: Name of staging table
            target_table: Name of target table
        """
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {DEFAULT_DB}.{DEFAULT_SCHEMA}.{staging_table} AS staging
        ON target.job_id = staging.job_id
           AND target.load_date = staging.load_date
        WHEN MATCHED THEN
            UPDATE SET
                job_title = staging.job_title,
                company = staging.company,
                salary_min = staging.salary_min,
                salary_max = staging.salary_max,
                salary_avg = staging.salary_avg,
                description = staging.description,
                posting_date = staging.posting_date,
                location = staging.location,
                city = staging.city,
                state = staging.state,
                category = staging.category,
                contract_type = staging.contract_type,
                contract_time = staging.contract_time,
                latitude = staging.latitude,
                longitude = staging.longitude,
                redirect_url = staging.redirect_url,
                extracted_at = staging.extracted_at
        WHEN NOT MATCHED THEN
            INSERT (
                job_id, job_title, company, salary_min, salary_max, salary_avg,
                description, posting_date, location, city, state, category,
                contract_type, contract_time, latitude, longitude, redirect_url,
                load_date, extracted_at
            )
            VALUES (
                staging.job_id, staging.job_title, staging.company,
                staging.salary_min, staging.salary_max, staging.salary_avg,
                staging.description, staging.posting_date, staging.location,
                staging.city, staging.state, staging.category,
                staging.contract_type, staging.contract_time,
                staging.latitude, staging.longitude, staging.redirect_url,
                staging.load_date, staging.extracted_at
            );
        """

        logger.info(f"Merging data from {staging_table} to {target_table}")
        # Use raw cursor.execute for merge
        conn = self.hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(merge_sql)
            conn.commit()
            logger.info("Merge completed successfully via cursor.execute")
        finally:
            cur.close()

    def get_load_statistics(self, target_table: str, execution_date: str) -> dict:
        """
        Get statistics about the loaded data

        Args:
            target_table: Name of target table
            execution_date: Airflow execution date (YYYY-MM-DD)

        Returns:
            Dictionary with statistics
        """
        stats_sql = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT job_id) as unique_jobs,
            COUNT(DISTINCT company) as unique_companies,
            COUNT(DISTINCT category) as unique_categories,
            MIN(posting_date) as earliest_posting,
            MAX(posting_date) as latest_posting,
            AVG(salary_min) as avg_min_salary,
            AVG(salary_max) as avg_max_salary,
            COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as jobs_with_salary
        FROM {target_table}
        WHERE load_date = '{execution_date}';
        """

        result = self.hook.get_first(stats_sql)

        if result:
            stats = {
                "total_records": result[0],
                "unique_jobs": result[1],
                "unique_companies": result[2],
                "unique_categories": result[3],
                "earliest_posting": str(result[4]) if result[4] else None,
                "latest_posting": str(result[5]) if result[5] else None,
                "avg_min_salary": float(result[6]) if result[6] is not None else None,
                "avg_max_salary": float(result[7]) if result[7] is not None else None,
                "jobs_with_salary": result[8],
            }
            return stats

        return {}


def load_to_snowflake(
    input_path: str,
    execution_date: str,
    conn_id: str = "snowflake_conn",
    **context,
) -> None:
    """
    Main loading function called by Airflow

    Args:
        input_path: Path to transformed CSV file
        execution_date: Airflow execution date (YYYY-MM-DD)
        conn_id: Snowflake connection ID
        **context: Airflow context
    """
    logger.info(f"Starting Snowflake load for {execution_date}")

    try:
        # Initialize loader
        loader = SnowflakeLoader(conn_id=conn_id)

        # Get table names
        target_table = loader.get_table_name()
        staging_table = loader.get_staging_table_name(execution_date)

        # Create staging table
        loader.create_staging_table(staging_table)

        # Load CSV to staging
        rows_loaded = loader.load_csv_to_staging(input_path, staging_table)

        # Merge staging to target
        if rows_loaded > 0:
            loader.merge_staging_to_target(staging_table, target_table)
        else:
            logger.info("No rows loaded; skipping merge step.")

        # Get statistics
        stats = loader.get_load_statistics(target_table, execution_date)

        # Log statistics
        logger.info("Load statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

        # Push statistics to XCom
        ti = context.get("task_instance")
        if ti is not None:
            ti.xcom_push(key="load_statistics", value=stats)
            ti.xcom_push(key="jobs_loaded_count", value=rows_loaded)

        logger.info(f"Successfully loaded {rows_loaded} jobs to Snowflake")

    except Exception as e:
        logger.error(f"Load failed: {str(e)}")
        raise


if __name__ == "__main__":
    # For testing purposes
    print("This module is designed to be used within Airflow")
