-- =============================================================================
-- Snowflake Table Schema for Adzuna Job Listings
-- =============================================================================

-- Ensure database and schema exist
CREATE DATABASE IF NOT EXISTS USER_DB_HYENA;
CREATE SCHEMA IF NOT EXISTS USER_DB_HYENA.RAW;
USE DATABASE USER_DB_HYENA;
USE SCHEMA RAW;
-- Create main job listings table
CREATE OR REPLACE TABLE job_listings (
    -- Primary identifiers
    job_id VARCHAR(255) NOT NULL,
    load_date DATE NOT NULL,
    
    -- Job details
    job_title VARCHAR(500) NOT NULL,
    company VARCHAR(500) NOT NULL,
    description TEXT,
    
    -- Salary information (critical for ROI forecasts)
    salary_min FLOAT,
    salary_max FLOAT,
    salary_avg FLOAT,
    
    -- Temporal data
    posting_date DATE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    -- Location data
    location VARCHAR(500),
    city VARCHAR(255),
    state VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    
    -- Job classification (critical for Skill Demand Index)
    category VARCHAR(255) NOT NULL,
    contract_type VARCHAR(100),
    contract_time VARCHAR(100),
    
    -- Additional metadata
    redirect_url TEXT,
    
    -- Composite primary key to handle daily updates
    PRIMARY KEY (job_id, load_date)
);

-- =============================================================================
-- Indexes for Performance Optimization
-- =============================================================================

-- Note: Snowflake does not support traditional CREATE INDEX; skipping index creation.
-- If clustering is needed, consider CLUSTER BY or materialized views in a follow-up step.

-- -- =============================================================================
-- -- Views for Analytics and Reporting
-- -- =============================================================================

-- Views omitted here to avoid permission and dependency issues during initial schema creation.
-- Create views in a separate migration after grants/privileges are validated.

-- View: Skill demand index by category
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

-- View: Salary trends over time by category
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

-- View: Location-based job market analysis
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

-- View: Company hiring trends
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

-- =============================================================================
-- Comments for Documentation
-- =============================================================================

-- Add comments immediately after table creation using unqualified names to ensure they run in the same schema/session.
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
