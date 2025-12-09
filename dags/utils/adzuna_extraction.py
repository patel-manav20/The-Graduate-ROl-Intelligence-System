"""
Adzuna API Data Extraction Module
==================================
Extracts job listings from Adzuna API for the United States market.

API Documentation: https://developer.adzuna.com/
Fields extracted: job_title, company, salary_min, salary_max, description, 
                 posting_date, location, category
"""

import requests
import json
import logging
from datetime import datetime
from typing import Dict, List, Any
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdzunaAPIClient:
    """Client for interacting with Adzuna Job Search API"""
    
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    COUNTRY = "us"  # United States
    
    def __init__(self):
        """Initialize API client with credentials from Airflow Variables"""
        try:
            self.app_id = Variable.get("adzuna_app_id")
            self.app_key = Variable.get("adzuna_app_key")
        except Exception as e:
            logger.error(f"Failed to retrieve Adzuna API credentials: {str(e)}")
            raise ValueError("Adzuna API credentials not found in Airflow Variables")
    
    def build_search_url(self, page: int = 1, results_per_page: int = 50) -> str:
        """
        Build the API URL for job search
        
        Args:
            page: Page number for pagination
            results_per_page: Number of results per page (max 50)
        
        Returns:
            Complete API URL
        """
        return (f"{self.BASE_URL}/{self.COUNTRY}/search/{page}"
                f"?app_id={self.app_id}&app_key={self.app_key}"
                f"&results_per_page={results_per_page}")
    
    def fetch_jobs_page(self, page: int = 1, results_per_page: int = 50) -> Dict[str, Any]:
        """
        Fetch a single page of job listings
        
        Args:
            page: Page number
            results_per_page: Number of results per page
            
        Returns:
            JSON response from API
        """
        url = self.build_search_url(page, results_per_page)
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            raise
    
    def extract_job_fields(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant fields from a job listing
        
        Args:
            job: Raw job listing from API
            
        Returns:
            Cleaned job data with required fields
        """
        return {
            'job_id': job.get('id', ''),
            'job_title': job.get('title', ''),
            'company': job.get('company', {}).get('display_name', ''),
            'salary_min': job.get('salary_min'),
            'salary_max': job.get('salary_max'),
            'description': job.get('description', ''),
            'posting_date': job.get('created', ''),
            'location': job.get('location', {}).get('display_name', ''),
            'category': job.get('category', {}).get('label', ''),
            'contract_type': job.get('contract_type', ''),
            'contract_time': job.get('contract_time', ''),
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude'),
            'redirect_url': job.get('redirect_url', ''),
        }
    
    def fetch_all_jobs(self, max_pages: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages of job listings
        
        Args:
            max_pages: Maximum number of pages to fetch (default 100 = 5000 jobs)
            
        Returns:
            List of all job listings
        """
        all_jobs = []
        
        for page in range(1, max_pages + 1):
            try:
                logger.info(f"Fetching page {page} of {max_pages}")
                response = self.fetch_jobs_page(page=page, results_per_page=50)
                
                jobs = response.get('results', [])
                if not jobs:
                    logger.info(f"No more jobs found at page {page}")
                    break
                
                # Extract relevant fields from each job
                cleaned_jobs = [self.extract_job_fields(job) for job in jobs]
                all_jobs.extend(cleaned_jobs)
                
                # Log progress
                total_count = response.get('count', 0)
                logger.info(f"Extracted {len(cleaned_jobs)} jobs from page {page}. "
                          f"Total so far: {len(all_jobs)}. API reports {total_count} total jobs.")
                
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {str(e)}")
                # Continue with next page instead of failing completely
                continue
        
        return all_jobs


def extract_adzuna_jobs(execution_date: str, output_path: str, **context) -> None:
    """
    Main extraction function called by Airflow
    
    Args:
        execution_date: Airflow execution date
        output_path: Path to save extracted data
        **context: Airflow context
    """
    logger.info(f"Starting Adzuna job extraction for {execution_date}")
    
    try:
        # Initialize API client
        client = AdzunaAPIClient()
        
        # Fetch jobs (adjust max_pages based on your needs)
        # 100 pages = ~5000 jobs, increase for more coverage
        max_pages = int(Variable.get("adzuna_max_pages", default_var=20))
        jobs = client.fetch_all_jobs(max_pages=max_pages)
        
        # Add extraction metadata
        extraction_metadata = {
            'extraction_date': datetime.utcnow().isoformat(),
            'execution_date': execution_date,
            'total_jobs_extracted': len(jobs),
            'jobs': jobs
        }
        
        # Save to JSON file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(extraction_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully extracted {len(jobs)} jobs to {output_path}")
        
        # Push metadata to XCom for downstream tasks
        context['task_instance'].xcom_push(
            key='jobs_extracted_count',
            value=len(jobs)
        )
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    # For testing purposes
    print("This module is designed to be used within Airflow")
    print("Configure Airflow Variables: adzuna_app_id, adzuna_app_key")
