"""
Adzuna Job Listings ETL Pipeline
=================================
This DAG extracts job listing data from Adzuna API, transforms it, and loads it into Snowflake.

Schedule: Daily at 2 AM UTC
Data Volume: ~1 million active US job listings
Purpose: Feed Skill Demand Index and salary trends for ROI forecasts
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
import json
from datetime import datetime
import requests
import os
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


@dag(
    dag_id='adzuna_job_listings_etl',
    default_args=default_args,
    description='ETL pipeline for Adzuna job listings to Snowflake',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2025, 12, 6),
    catchup=False,
    max_active_runs=1,
    tags=['adzuna', 'jobs', 'etl', 'snowflake'],
)
def adzuna_etl_pipeline():
    
    
    import os
    data_dir = os.getenv('AIRFLOW_DATA_DIR', '/opt/airflow/data')
    sql_dir = os.getenv('AIRFLOW_SQL_DIR', '/opt/airflow/sql')
    
    @task(task_id='extract_adzuna_jobs')
    def extract_jobs(date_str: str) -> str:
        """
        Extract job listings from Adzuna API
        
        Args:
            date_str: Airflow execution date (ds)
            
        Returns:
            Path to raw JSON file
        """
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting Adzuna job extraction for {date_str}")
        
        # Get API credentials and extraction settings from Airflow Variables
        app_id = Variable.get("adzuna_app_id")
        app_key = Variable.get("adzuna_app_key")
        
        pages_per_category = int(Variable.get("adzuna_pages_per_category", default_var=15))

        
        categories = [
            "accounting-finance-jobs",
            "it-jobs",
            "sales-jobs",
            "customer-services-jobs",
            "engineering-jobs",
            "hr-jobs",
            "healthcare-nursing-jobs",
            "hospitality-catering-jobs",
            "pr-advertising-marketing-jobs",
            "logistics-warehouse-jobs",
            "teaching-jobs",
            "trade-construction-jobs",
            "admin-jobs",
            "legal-jobs",
            "creative-design-jobs",
            "graduate-jobs",
            "retail-jobs",
            "consultancy-jobs",
            "manufacturing-jobs",
            "scientific-qa-jobs",
            "social-work-jobs",
            "travel-jobs",
            "energy-oil-gas-jobs",
            "property-jobs",
            "charity-voluntary-jobs",
            "domestic-help-cleaning-jobs",
            "maintenance-jobs",
            "part-time-jobs",
            "other-general-jobs",
            "unknown",
        ]
        
        # Adzuna API configuration
        BASE_URL = "https://api.adzuna.com/v1/api/jobs"
        COUNTRY = "us"
        
        def fetch_jobs_page(page: int, category: str = None, results_per_page: int = 50):
            """Fetch a single page of job listings using the 'category' parameter."""
            # Build URL like: /search/{page}?app_id=...&app_key=...&results_per_page=10&category=...
            url = f"{BASE_URL}/{COUNTRY}/search/{page}"
            params = {
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": results_per_page,
            }
            if category:
                # pass human-friendly category as 'category' param (requests will URL-encode)
                params["category"] = category

            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()

                # Try to parse JSON, but be robust to non-dict JSON (or HTML/text responses)
                try:
                    data = response.json()
                except ValueError:
                    logger.error("Adzuna returned non-JSON response (status=%s). Response text: %s",
                                 response.status_code, response.text[:500])
                    return {"results": []}

                # Normalize types: if Adzuna returns a list, wrap it under 'results'
                if isinstance(data, dict):
                    return data
                if isinstance(data, list):
                    return {"results": data}

                logger.error("Unexpected JSON payload type from Adzuna: %s", type(data))
                return {"results": []}
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page} (category={category}): {str(e)}")
                raise
        
        def extract_job_fields(job: dict) -> dict:
            """Extract relevant fields from a job listing. Handles category as dict or string."""
            # Normalize category: API may provide a dict with 'label' and 'tag' or a simple string
            raw_cat = job.get('category', '')
            if isinstance(raw_cat, dict):
                category_val = raw_cat.get('tag') or raw_cat.get('label') or ''
            else:
                category_val = raw_cat or ''

            return {
                'job_id': job.get('id', ''),
                'job_title': job.get('title', ''),
                'company': job.get('company', {}).get('display_name', ''),
                'salary_min': job.get('salary_min'),
                'salary_max': job.get('salary_max'),
                'description': job.get('description', ''),
                'posting_date': job.get('created', ''),
                'location': job.get('location', {}).get('display_name', ''),
                'category': category_val,
                'contract_type': job.get('contract_type', ''),
                'contract_time': job.get('contract_time', ''),
                'latitude': job.get('latitude'),
                'longitude': job.get('longitude'),
                'redirect_url': job.get('redirect_url', ''),
            }
        
        
        all_jobs = []
        for category in categories:
            logger.info(f"Starting extraction for category: {category}")
            for page in range(1, pages_per_category + 1):
                try:
                    logger.info(f"Fetching category={category} page {page}/{pages_per_category}")
                    
                    response = fetch_jobs_page(page=page, category=category)

                    if isinstance(response, dict):
                        jobs = response.get('results') or []
                    else:
                        logger.error("Unexpected response type from fetch_jobs_page: %s", type(response))
                        jobs = []
                    if not jobs:
                        logger.info(f"No results for category={category} on page {page}; stopping early for this category")
                        break

                    
                    cleaned_jobs = []
                    for job in jobs:
                        if isinstance(job.get('category'), dict):
                            job_category_tag = job.get('category', {}).get('tag') or category
                        else:
                            
                            job_category_tag = job.get('category') or category
                        
                        job['category'] = job_category_tag
                        cleaned_jobs.append(extract_job_fields(job))

                    all_jobs.extend(cleaned_jobs)
                    logger.info(f"Extracted {len(cleaned_jobs)} jobs from category={category} page {page}. Total so far: {len(all_jobs)}")

                except Exception as e:
                    logger.error(f"Failed to fetch category={category} page {page}: {str(e)}")
                   
                    continue
        
        # Save to JSON file
        output_path = f"{data_dir}/raw_jobs_{date_str}.json"
        os.makedirs(data_dir, exist_ok=True)
        
        extraction_metadata = {
            'extraction_date': datetime.utcnow().isoformat(),
            'execution_date': date_str,
            'total_jobs_extracted': len(all_jobs),
            'jobs': all_jobs
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(extraction_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully extracted {len(all_jobs)} jobs to {output_path}")
        
        return output_path
    
    @task(task_id='transform_job_data')
    def transform_jobs(input_path: str, date_str: str) -> str:
        """
        Transform raw job data into clean format
        
        Args:
            input_path: Path to raw JSON file
            date_str: Airflow execution date
            
        Returns:
            Path to transformed CSV file
        """
        import json
        import pandas as pd
        import logging
        from datetime import datetime
        import re
        from html import unescape
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting job data transformation for {date_str}")
        
        # Load raw data
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        jobs = raw_data.get('jobs', [])
        logger.info(f"Loaded {len(jobs)} jobs from {input_path}")
        
        def clean_html(text: str) -> str:
            """Remove HTML tags and clean text"""
            if not text or not isinstance(text, str):
                return ""
            text = unescape(text)
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        
        def parse_date(date_str: str) -> str:
            """Parse and standardize date format"""
            if not date_str:
                return None
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except Exception:
                return None
        
        def normalize_salary(salary_min, salary_max):
            """Normalize salary values"""
            def clean_salary(value):
                if value is None or pd.isna(value):
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            min_sal = clean_salary(salary_min)
            max_sal = clean_salary(salary_max)
            
            if min_sal and max_sal and min_sal > max_sal:
                min_sal, max_sal = max_sal, min_sal
            
            return min_sal, max_sal
        
        def clean_location(location: str):
            """Parse and clean location data"""
            if not location:
                return {'city': None, 'state': None}
            
            location = location.strip()
            if ',' in location:
                parts = [p.strip() for p in location.split(',')]
                city = parts[0] if len(parts) > 0 else None
                state = parts[1] if len(parts) > 1 else None
            else:
                city = location
                state = None
            
            return {'city': city, 'state': state}
        
        # Transform jobs
        transformed_jobs = []
        for job in jobs:
            if not job.get('job_id') or not job.get('job_title'):
                continue
            
            location_data = clean_location(job.get('location', ''))
            salary_min, salary_max = normalize_salary(
                job.get('salary_min'),
                job.get('salary_max')
            )
            
            salary_avg = None
            if salary_min and salary_max:
                salary_avg = (salary_min + salary_max) / 2
            
            transformed_job = {
                'job_id': str(job.get('job_id', '')),
                'job_title': clean_html(job.get('job_title', '')),
                'company': clean_html(job.get('company', '')),
                'salary_min': salary_min,
                'salary_max': salary_max,
                'salary_avg': salary_avg,
                'description': clean_html(job.get('description', '')),
                'posting_date': parse_date(job.get('posting_date', '')),
                'location': job.get('location', ''),
                'city': location_data['city'],
                'state': location_data['state'],
                'category': job.get('category', ''),
                'contract_type': job.get('contract_type', ''),
                'contract_time': job.get('contract_time', ''),
                'latitude': job.get('latitude'),
                'longitude': job.get('longitude'),
                'redirect_url': job.get('redirect_url', ''),
                'load_date': date_str,
                'extracted_at': datetime.utcnow().isoformat(),
            }
            transformed_jobs.append(transformed_job)
        
        # Create DataFrame
        df = pd.DataFrame(transformed_jobs)
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['job_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate jobs")
        
        # Save to CSV
        output_path = f"{data_dir}/transformed_jobs_{date_str}.csv"
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Successfully transformed {len(df)} jobs to {output_path}")
        logger.info(f"  Jobs with salary data: {df['salary_min'].notna().sum()}")
        logger.info(f"  Unique companies: {df['company'].nunique()}")
        logger.info(f"  Unique categories: {df['category'].nunique()}")
        
        return output_path
    
    @task(task_id='load_to_snowflake')
    def load_jobs(input_path: str, date_str: str) -> dict:
        """
        Load transformed data into Snowflake with transaction management
        
        Args:
            input_path: Path to transformed CSV file
            date_str: Airflow execution date
            
        Returns:
            Load statistics dictionary
        """
        
        
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting Snowflake load for {date_str}")
        
        # Initialize Snowflake hook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Get table names with fully qualified names
        target_table = "USER_DB_HYENA.RAW.job_listings"
        date_suffix = date_str.replace('-', '_')
        staging_table = f"job_listings_staging_{date_suffix}"  # Temp tables don't need schema prefix
        
        # Read CSV
        df = pd.read_csv(input_path)
        logger.info(f"Read {len(df)} rows from CSV")

        # Convert numeric NaN (numpy.nan), pandas NA, and string variants ('nan','None','null','n/a') -> None
        import numpy as np
        import math

        # Ensure object dtype so we can place None safely
        try:
            df = df.astype(object)
        except Exception:
            # if astype fails, continue with existing dtypes
            pass

        def _clean_value(v):
            if v is None:
                return None
            
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
            # String variants
            if isinstance(v, str):
                if v.strip().lower() in {"nan", "none", "null", "na", "n/a", "nan.0"}:
                    return None
                
                return v
            return v

        # Apply cleaning (row/column-wise)
        df = df.applymap(_clean_value)

        # convert DataFrame rows to tuples
        records = df.to_records(index=False)
        data = [tuple(record) for record in records]
        
        # Get connection
        conn = hook.get_conn()
        db_executor = conn.cursor()

        try:
            # BEGIN TRANSACTION
            logger.info("Beginning transaction")
            db_executor.execute("BEGIN")

            
            try:
               
                parts = target_table.split('.')
                if len(parts) >= 3:
                    db_name = parts[0]
                    schema_name = parts[1]
                elif len(parts) == 2:
                    db_name = parts[0]
                    schema_name = None
                else:
                    db_name = None
                    schema_name = None

                if db_name:
                    logger.info(f"Setting session database to: {db_name}")
                    db_executor.execute(f"USE DATABASE {db_name}")
                if schema_name:
                    logger.info(f"Setting session schema to: {schema_name}")
                    db_executor.execute(f"USE SCHEMA {schema_name}")

            except Exception as e:
                logger.warning(f"Could not set session DB/SCHEMA from target_table: {e}")

            # Build fully-qualified staging table name to avoid session-dependence
            if db_name and schema_name:
                staging_qualified = f"{db_name}.{schema_name}.{staging_table}"
            elif schema_name:
                staging_qualified = f"{schema_name}.{staging_table}"
            else:
                staging_qualified = staging_table

            # Create staging table (fully-qualified)
            create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_qualified} (
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
            logger.info(f"Creating staging table: {staging_qualified}")
            db_executor.execute(create_staging_sql)

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

            # Normalize column names to strings
            df.columns = [str(c).strip() for c in df.columns]

            # Keep only expected columns; drop everything else (including NaN/Unnamed)
            extra_cols = [c for c in df.columns if c not in expected_columns]
            if extra_cols:
                logger.warning(f"Dropping unexpected columns from CSV: {extra_cols}")
                df = df[[c for c in df.columns if c in expected_columns]]

            # Add missing expected columns as None
            missing_cols = [c for c in expected_columns if c not in df.columns]
            if missing_cols:
                logger.warning(f"Adding missing columns with NULLs: {missing_cols}")
                for c in missing_cols:
                    df[c] = None

            # Reorder columns to match staging table schema
            df = df[expected_columns]

            
            try:
                df = df.where(pd.notnull(df), None)
            except Exception:
                
                df = df.applymap(lambda x: None if (isinstance(x, float) and math.isnan(x)) else x)

            
            records = df.to_records(index=False)
            data = [tuple(record) for record in records]

            columns_unquoted = ', '.join([c.upper() for c in expected_columns])
            logger.info(f"Inserting data into staging table {staging_qualified} with columns: {columns_unquoted}")
            column_map = {i: col for i, col in enumerate(expected_columns)}

            logger.info(
                f"Inserting data into staging table {staging_qualified} "
                f"with column index map: {column_map}"
            )
            placeholders = ', '.join(['%s'] * len(expected_columns))
            
            insert_sql = f"INSERT INTO {staging_qualified} ({columns_unquoted}) VALUES ({placeholders})"

            batch_size = 1000
            total_inserted = 0
            def sanitize_row(row):
                cleaned = []
                for v in row:
                    if isinstance(v, float) and math.isnan(v):
                        cleaned.append(None)  
                    else:
                        cleaned.append(v)
                return tuple(cleaned)

            data_clean = [sanitize_row(r) for r in data]

            for i in range(0, len(data_clean), batch_size):
                batch = data_clean[i:i + batch_size]
                db_executor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}: {total_inserted}/{len(data)} rows")

          
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {staging_qualified} AS staging
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
            logger.info(f"Merging data from {staging_qualified} to {target_table}")
            db_executor.execute(merge_sql)
            
            # Get load statistics
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
            WHERE load_date = '{date_str}';
            """
            db_executor.execute(stats_sql)
            result = db_executor.fetchone()
            
            stats = {
                'total_records': result[0],
                'unique_jobs': result[1],
                'unique_companies': result[2],
                'unique_categories': result[3],
                'earliest_posting': str(result[4]) if result[4] else None,
                'latest_posting': str(result[5]) if result[5] else None,
                'avg_min_salary': float(result[6]) if result[6] else None,
                'avg_max_salary': float(result[7]) if result[7] else None,
                'jobs_with_salary': result[8],
            }
            
            # COMMIT TRANSACTION
            logger.info("Committing transaction")
            db_executor.execute("COMMIT")
            
            logger.info(f"Successfully loaded {total_inserted} jobs to Snowflake")
            logger.info("Load statistics:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
            
            return stats
            
        except Exception as e:
            # ROLLBACK on error
            logger.error(f"Load failed, rolling back transaction: {str(e)}")
            db_executor.execute("ROLLBACK")
            raise
        finally:
            db_executor.close()
    
    # Task 1: Create/Update Snowflake table schema
    @task(task_id='create_snowflake_table')
    def create_table():
        """Create Snowflake table schema"""
        
        logger = logging.getLogger(__name__)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

        # SQL statements embedded directly
        sql_content = """
CREATE DATABASE IF NOT EXISTS USER_DB_HYENA;
CREATE SCHEMA IF NOT EXISTS USER_DB_HYENA.RAW;
USE DATABASE USER_DB_HYENA;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE job_listings (
    job_id VARCHAR(255) NOT NULL,
    load_date DATE NOT NULL,
    job_title VARCHAR(500) NOT NULL,
    company VARCHAR(500) NOT NULL,
    description TEXT,
    salary_min FLOAT,
    salary_max FLOAT,
    salary_avg FLOAT,
    posting_date DATE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    location VARCHAR(500),
    city VARCHAR(255),
    state VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    category VARCHAR(255) NOT NULL,
    contract_type VARCHAR(100),
    contract_time VARCHAR(100),
    redirect_url TEXT,
    PRIMARY KEY (job_id, load_date)
);

CREATE OR REPLACE VIEW USER_DB_HYENA.RAW.vw_skill_demand_index AS
SELECT 
    category,
    load_date,
    COUNT(*) as job_count,
    COUNT(DISTINCT company) as company_count,
    AVG(salary_avg) as avg_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_avg) as median_salary,
    MIN(salary_min) as min_salary,
    MAX(salary_max) as max_salary,
    COUNT(CASE WHEN salary_avg IS NOT NULL THEN 1 END) as jobs_with_salary
FROM USER_DB_HYENA.RAW.job_listings
WHERE category IS NOT NULL
GROUP BY category, load_date;

CREATE OR REPLACE VIEW USER_DB_HYENA.RAW.vw_salary_trends AS
SELECT 
    category,
    posting_date,
    COUNT(*) as job_count,
    AVG(salary_avg) as avg_salary,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_avg) as q1_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_avg) as median_salary,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_avg) as q3_salary
FROM USER_DB_HYENA.RAW.job_listings
WHERE salary_avg IS NOT NULL 
  AND posting_date IS NOT NULL
GROUP BY category, posting_date;

CREATE OR REPLACE VIEW USER_DB_HYENA.RAW.vw_job_market_by_location AS
SELECT 
    state,
    city,
    load_date,
    COUNT(*) as job_count,
    COUNT(DISTINCT category) as category_count,
    COUNT(DISTINCT company) as company_count,
    AVG(salary_avg) as avg_salary
FROM USER_DB_HYENA.RAW.job_listings
WHERE state IS NOT NULL
GROUP BY state, city, load_date;

CREATE OR REPLACE VIEW USER_DB_HYENA.RAW.vw_company_hiring_trends AS
SELECT 
    company,
    load_date,
    COUNT(*) as active_positions,
    COUNT(DISTINCT category) as categories_hiring,
    AVG(salary_avg) as avg_offered_salary,
    COUNT(DISTINCT state) as locations_count
FROM USER_DB_HYENA.RAW.job_listings
WHERE company IS NOT NULL
GROUP BY company, load_date;

COMMENT ON TABLE job_listings IS 
'Main table storing job listings from Adzuna API. Updated daily with ~1M active US listings. Used for Skill Demand Index and ROI forecasts.';

COMMENT ON COLUMN job_listings.job_id IS 
'Unique identifier from Adzuna API';

COMMENT ON COLUMN job_listings.load_date IS 
'Date when this record was loaded into Snowflake (partition key)';

COMMENT ON COLUMN job_listings.category IS 
'Job category/skill area - critical for Skill Demand Index calculation';

COMMENT ON COLUMN job_listings.salary_avg IS 
'Average of salary_min and salary_max - used for ROI forecasting';
        """

        # --- Split SQL by semicolon into individual statements ---
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

        conn = hook.get_conn()
        executor = conn.cursor()

        try:
            logger.info("Beginning CREATE/USE/COMMENT execution")

            
            for stmt in list(statements):
                upper = stmt.upper()
                if upper.startswith("USE ") or upper.startswith("SET "):
                    logger.info(f"Executing session stmt: {stmt[:120]}...")
                    cur.execute(stmt)
                    # remove executed stmt from list
                    statements.remove(stmt)

            
            import re

            def _strip_leading_comments_and_ws(text: str) -> str:
               
                t = re.sub(r'^(\s*--.*(\r?\n))+', '', text, flags=re.IGNORECASE)
                return t.lstrip()

            def _is_create_table(stmt: str) -> bool:
                s = _strip_leading_comments_and_ws(stmt).upper()
                return s.startswith("CREATE OR REPLACE TABLE") or s.startswith("CREATE TABLE")

            table_creates = [s for s in statements if _is_create_table(s)]
            logger.info(f"table_creates: {table_creates}")

            other_creates = [s for s in statements if s not in table_creates and _strip_leading_comments_and_ws(s).upper().startswith("CREATE")]
            comment_stmts = [s for s in statements if _strip_leading_comments_and_ws(s).upper().startswith("COMMENT")]
            other_stmts = [s for s in statements if s not in table_creates + other_creates + comment_stmts]

            # 1) Create tables
            for stmt in table_creates:
                logger.info(f"Executing CREATE TABLE: {stmt[:120]}...")
                cur.execute(stmt)

            # 2) Create other objects (views, etc.)
            for stmt in other_creates:
                logger.info(f"Executing CREATE statement: {stmt[:120]}...")
                cur.execute(stmt)

            # 3) Run remaining statements (ALTER/GRANT/etc.)
            for stmt in other_stmts:
                upper = stmt.upper()
                if upper.startswith("ALTER") or upper.startswith("GRANT") or upper.startswith("REVOKE"):
                    logger.info(f"Executing DDL: {stmt[:120]}...")
                    cur.execute(stmt)

            
            for stmt in comment_stmts:
                logger.info(f"Executing COMMENT: {stmt[:120]}...")
                cur.execute(stmt)

            logger.info("Schema created successfully")

        except Exception as e:
            logger.error(f"Error during schema creation: {e}")
            raise

        finally:
            cur.close()
            conn.close()

        return True
    
    # Task 5: Data quality checks
    quality_check_task = SnowflakeOperator(
        task_id='data_quality_checks',
        snowflake_conn_id='snowflake_conn',
        sql="""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT job_id) as unique_jobs,
            MIN(posting_date) as earliest_date,
            MAX(posting_date) as latest_date,
            AVG(salary_min) as avg_min_salary,
            AVG(salary_max) as avg_max_salary
        FROM USER_DB_HYENA.RAW.job_listings
        WHERE load_date = '{{ ds }}';
        """,
    )
    
    # Define task dependencies using TaskFlow API
    # Call each task once and chain them properly
    schema_created = create_table()
    raw_data_path = extract_jobs('{{ ds }}')
    transformed_data_path = transform_jobs(raw_data_path, '{{ ds }}')
    load_stats = load_jobs(transformed_data_path, '{{ ds }}')
    
    # Set dependencies
    schema_created >> raw_data_path >> transformed_data_path >> load_stats >> quality_check_task


# Instantiate the DAG
adzuna_etl_dag = adzuna_etl_pipeline()
