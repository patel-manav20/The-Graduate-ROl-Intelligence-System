"""
Adzuna Job Data Transformation Module
======================================
Transforms raw job listing data from Adzuna API into clean format for Snowflake.

Transformations:
- Data cleaning and validation
- Date parsing and standardization
- Salary normalization
- Text cleaning for descriptions
- Deduplication
"""

import json
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any
import re
from html import unescape

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobDataTransformer:
    """Transformer for Adzuna job listing data"""
    
    def __init__(self):
        """Initialize transformer"""
        self.required_fields = [
            'job_id', 'job_title', 'company', 'location', 'category'
        ]
    
    def clean_html(self, text: str) -> str:
        """
        Remove HTML tags and clean text
        
        Args:
            text: Raw text potentially containing HTML
            
        Returns:
            Cleaned text
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Unescape HTML entities
        text = unescape(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove multiple spaces and newlines
        text = re.sub(r'\s+', ' ', text)
        
        # Trim
        text = text.strip()
        
        return text
    
    def parse_date(self, date_str: str) -> str:
        """
        Parse and standardize date format
        
        Args:
            date_str: Date string from API
            
        Returns:
            ISO format date string (YYYY-MM-DD)
        """
        if not date_str:
            return None
        
        try:
            # Adzuna typically returns ISO format
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {str(e)}")
            return None
    
    def normalize_salary(self, salary_min: Any, salary_max: Any) -> tuple:
        """
        Normalize salary values
        
        Args:
            salary_min: Minimum salary
            salary_max: Maximum salary
            
        Returns:
            Tuple of (normalized_min, normalized_max)
        """
        def clean_salary(value):
            if value is None or pd.isna(value):
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        min_sal = clean_salary(salary_min)
        max_sal = clean_salary(salary_max)
        
        # Ensure min <= max
        if min_sal and max_sal and min_sal > max_sal:
            min_sal, max_sal = max_sal, min_sal
        
        return min_sal, max_sal
    
    def clean_location(self, location: str) -> Dict[str, str]:
        """
        Parse and clean location data
        
        Args:
            location: Location string from API
            
        Returns:
            Dictionary with city and state
        """
        if not location:
            return {'city': None, 'state': None}
        
        location = location.strip()
        
        # Common format: "City, State" or "City, ST"
        if ',' in location:
            parts = [p.strip() for p in location.split(',')]
            city = parts[0] if len(parts) > 0 else None
            state = parts[1] if len(parts) > 1 else None
        else:
            city = location
            state = None
        
        return {'city': city, 'state': state}
    
    def validate_job(self, job: Dict[str, Any]) -> bool:
        """
        Validate that job has required fields
        
        Args:
            job: Job dictionary
            
        Returns:
            True if valid, False otherwise
        """
        for field in self.required_fields:
            if not job.get(field):
                return False
        return True
    
    def transform_job(self, job: Dict[str, Any], execution_date: str) -> Dict[str, Any]:
        """
        Transform a single job listing
        
        Args:
            job: Raw job data
            execution_date: Airflow execution date
            
        Returns:
            Transformed job data
        """
        # Parse location
        location_data = self.clean_location(job.get('location', ''))
        
        # Normalize salary
        salary_min, salary_max = self.normalize_salary(
            job.get('salary_min'),
            job.get('salary_max')
        )
        
        # Calculate average salary if both min and max available
        salary_avg = None
        if salary_min and salary_max:
            salary_avg = (salary_min + salary_max) / 2
        
        return {
            'job_id': str(job.get('job_id', '')),
            'job_title': self.clean_html(job.get('job_title', '')),
            'company': self.clean_html(job.get('company', '')),
            'salary_min': salary_min,
            'salary_max': salary_max,
            'salary_avg': salary_avg,
            'description': self.clean_html(job.get('description', '')),
            'posting_date': self.parse_date(job.get('posting_date', '')),
            'location': job.get('location', ''),
            'city': location_data['city'],
            'state': location_data['state'],
            'category': job.get('category', ''),
            'contract_type': job.get('contract_type', ''),
            'contract_time': job.get('contract_time', ''),
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude'),
            'redirect_url': job.get('redirect_url', ''),
            'load_date': execution_date,
            'extracted_at': datetime.utcnow().isoformat(),
        }
    
    def transform_jobs(self, jobs: List[Dict[str, Any]], execution_date: str) -> pd.DataFrame:
        """
        Transform list of jobs into DataFrame
        
        Args:
            jobs: List of raw job data
            execution_date: Airflow execution date
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(jobs)} jobs")
        
        transformed_jobs = []
        invalid_count = 0
        
        for job in jobs:
            if not self.validate_job(job):
                invalid_count += 1
                continue
            
            try:
                transformed_job = self.transform_job(job, execution_date)
                transformed_jobs.append(transformed_job)
            except Exception as e:
                logger.error(f"Failed to transform job {job.get('job_id')}: {str(e)}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_jobs)} jobs")
        logger.info(f"Skipped {invalid_count} invalid jobs")
        
        # Create DataFrame
        df = pd.DataFrame(transformed_jobs)
        
        # Remove duplicates based on job_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=['job_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate jobs")
        
        return df


def transform_job_data(input_path: str, output_path: str, execution_date: str, **context) -> None:
    """
    Main transformation function called by Airflow
    
    Args:
        input_path: Path to raw JSON data
        output_path: Path to save transformed CSV
        execution_date: Airflow execution date
        **context: Airflow context
    """
    logger.info(f"Starting job data transformation for {execution_date}")
    
    try:
        # Load raw data
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        jobs = raw_data.get('jobs', [])
        logger.info(f"Loaded {len(jobs)} jobs from {input_path}")
        
        # Transform data
        transformer = JobDataTransformer()
        df = transformer.transform_jobs(jobs, execution_date)
        
        # Save to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Successfully saved {len(df)} transformed jobs to {output_path}")
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key='jobs_transformed_count',
            value=len(df)
        )
        
        # Log statistics
        logger.info("Transformation statistics:")
        logger.info(f"  Total jobs transformed: {len(df)}")
        logger.info(f"  Jobs with salary data: {df['salary_min'].notna().sum()}")
        logger.info(f"  Unique companies: {df['company'].nunique()}")
        logger.info(f"  Unique categories: {df['category'].nunique()}")
        logger.info(f"  Date range: {df['posting_date'].min()} to {df['posting_date'].max()}")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


if __name__ == "__main__":
    # For testing purposes
    print("This module is designed to be used within Airflow")
