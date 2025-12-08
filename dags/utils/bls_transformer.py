"""
BLS Employment Projections Data Transformer
Transforms the CSV by splitting occupation titles with multiple job titles
"""
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

# File paths - using Docker mounted volume paths
# In Docker: /opt/airflow/dags maps to local dags/ directory
INPUT_FILE_PATH = '/opt/airflow/dags/data/Employment Projections.csv'
OUTPUT_FILE_PATH = '/opt/airflow/dags/data/transformed/Employment_Projections_Transformed.csv'


def transform_bls_data():
   
    logger.info(f"Reading BLS data from {INPUT_FILE_PATH}")
    
    
    if not os.path.exists(INPUT_FILE_PATH):
        local_path = '/Users/manavnayanbhaipatel/Downloads/Employment Projections.csv'
        if os.path.exists(local_path):
            logger.info(f"Using local path: {local_path}")
            df = pd.read_csv(local_path)
        else:
            raise FileNotFoundError(f"Input file not found at {INPUT_FILE_PATH} or {local_path}")
    else:
        # Read the original CSV file
        df = pd.read_csv(INPUT_FILE_PATH)
    
    logger.info(f"Original data shape: {df.shape}")
    
    transformed_rows = []
    
    # Process each row in the original data
    for idx, row in df.iterrows():
        occupation_title = str(row['Occupation Title']).strip()
        
        # Skip empty rows
        if not occupation_title or occupation_title == 'nan':
            continue
        
        parts = [part.strip() for part in occupation_title.split('*') if part.strip()]
        
        if len(parts) == 0:
            continue
        
        
        main_occupation_title = parts[0]
        
       
        if len(parts) == 1:
            job_titles = [main_occupation_title]
        else:
            job_titles = parts[1:] if len(parts) > 1 else [main_occupation_title]
        
        # Create a new row for each job title
        for job_title in job_titles:
            if job_title:
                new_row = row.copy()
                new_row['Occupation Title'] = main_occupation_title
                new_row['Job Title'] = job_title
                transformed_rows.append(new_row)
    
    # Create DataFrame from transformed rows
    transformed_df = pd.DataFrame(transformed_rows)
    
    # Reorder columns: Occupation Title and Job Title first
    columns = ['Occupation Title', 'Job Title'] + [col for col in transformed_df.columns 
                                                    if col not in ['Occupation Title', 'Job Title']]
    transformed_df = transformed_df[columns]
    
    # Replace NULL/None/NaN values in Work Experience column with "0"
    work_exp_col = 'Work Experience in a Related Occupation'
    if work_exp_col in transformed_df.columns:
        transformed_df[work_exp_col] = transformed_df[work_exp_col].fillna('0')
        transformed_df[work_exp_col] = transformed_df[work_exp_col].replace(['None', 'null', 'NULL', ''], '0')
        logger.info(f"Replaced NULL values in {work_exp_col} with '0'")
    
    logger.info(f"Transformed data shape: {transformed_df.shape}")
    
    # Save transformed data to output file
    output_dir = os.path.dirname(OUTPUT_FILE_PATH)
    os.makedirs(output_dir, exist_ok=True)
    transformed_df.to_csv(OUTPUT_FILE_PATH, index=False)
    logger.info(f"Transformed data saved to {OUTPUT_FILE_PATH}")
    
    return transformed_df
