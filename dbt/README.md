# Graduate ROI Intelligence System - Complete Data Pipeline

A comprehensive data warehouse system that extracts, transforms, and loads data from multiple sources into Snowflake, then transforms it into analytical marts for ROI analysis and career intelligence.

## 📋 Overview

This project consists of two main components:

1. **ETL Pipelines (Apache Airflow)**: Extract data from external sources and load into Snowflake RAW schema
2. **dbt Transformations**: Transform raw data into analytical marts for reporting and analysis

### Data Pipeline Summary

| Pipeline | Data Source | Schedule | Schema | Table |
|----------|-------------|----------|--------|-------|
| **BLS ETL** | BLS Employment Projections CSV | Monthly | RAW | BLS_EMPLOYMENT_PROJECTIONS |
| **Adzuna ETL** | Adzuna Job API | Daily (2 AM UTC) | RAW | JOB_LISTINGS |
| **College Scorecard ETL** | Dept. of Education API | Daily | RAW | COLLEGE_SCORECARD_DATA |
| **WARN ETL** | WARN Events CSV | Monthly | RAW | WARN_EVENTS |
| **dbt Transformations** | RAW Schema Tables | Daily | ANALYTICS | mart_degree_roi_and_industry_outlook |

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  BLS CSV    │     │ Adzuna API   │     │  College    │     │  WARN CSV   │
│             │     │             │     │  Scorecard  │     │             │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Docker)                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ BLS DAG  │  │ Adzuna   │  │ College  │  │ WARN DAG  │        │
│  │          │  │ DAG      │  │ DAG      │  │           │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬───────┘        │
└───────┼─────────────┼─────────────┼─────────────┼─────────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   Snowflake Warehouse  │
              │  USER_DB_HYENA.RAW     │
              │  (Source Tables)       │
              └────────────┬────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │      dbt Project       │
              │  ┌──────────────────┐  │
              │  │ Staging Models   │  │
              │  │ (Views in RAW)   │  │
              │  └────────┬─────────┘  │
              │           │            │
              │  ┌────────▼─────────┐  │
              │  │  Mart Models     │  │
              │  │ (Tables in       │  │
              │  │  ANALYTICS)      │  │
              │  └──────────────────┘  │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   Analytics & Reports  │
              │   Frontend App         │
              └────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Snowflake account
- API keys (for Adzuna and College Scorecard)
- dbt Core (version 1.0+)
- Python 3.8+

### Installation

#### 1. ETL Pipelines Setup

1. **Clone and navigate to project**
   ```bash
   cd "Group Project"
   ```

2. **Set up environment variables**
   ```bash
   export AIRFLOW_UID=$(id -u)
   ```

3. **Start Airflow**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

5. **Configure Airflow Connections**

   Navigate to Admin → Connections → Add:
   
   - **Connection ID**: `snowflake_conn`
   - **Connection Type**: Snowflake
   - **Account**: Your Snowflake account
   - **User**: HYENA
   - **Password**: Your password
   - **Warehouse**: HYENA_QUERRY_WH
   - **Database**: USER_DB_HYENA
   - **Schema**: RAW
   - **Role**: TRAINING_ROLE

6. **Configure Airflow Variables**

   Navigate to Admin → Variables → Add:
   
   | Key | Value |
   |-----|-------|
   | `adzuna_app_id` | Your Adzuna App ID |
   | `adzuna_app_key` | Your Adzuna App Key |
   | `adzuna_pages_per_category` | 15 |
   | `COLLEGE_SCORECARD_API_KEY` | Your API key |

7. **Place data files**
   ```bash
   # BLS data
   cp "Employment Projections.csv" dags/data/Employment\ Projections.csv
   
   # WARN data
   cp warn_events.csv dags/data/warn_events.csv
   ```

8. **Enable DAGs**
   - Open Airflow UI
   - Toggle ON the DAGs you want to run:
     - `bls_employment_projections_etl`
     - `adzuna_job_listings_etl`
     - `college_scorecard_etl`
     - `warn_etl`

#### 2. dbt Setup

1. **Navigate to dbt directory**
   ```bash
   cd dbt
   ```

2. **Set up environment variables**
   ```bash
   export DBT_ACCOUNT="your_snowflake_account"
   export DBT_USER="your_username"
   export DBT_PASSWORD="your_password"
   export DBT_DATABASE="USER_DB_HYENA"
   export DBT_SCHEMA="RAW"
   export DBT_WAREHOUSE="HYENA_QUERRY_WH"
   export DBT_ROLE="TRAINING_ROLE"
   ```

3. **Install dbt dependencies**
   ```bash
   dbt deps
   ```

4. **Test connection**
   ```bash
   dbt debug
   ```

5. **Run transformations**
   ```bash
   dbt run
   ```

## 📊 ETL Pipeline Details

### 1. BLS Employment Projections ETL

**Purpose**: Extract employment projections and wage data for ROI forecasts

**Schedule**: Monthly (`@monthly`)

**Tasks**:
1. Create Snowflake table schema
2. Extract & Transform: Split occupation titles into individual job titles
3. Load: Batch load to Snowflake using staging + MERGE pattern
4. Data quality checks

**Key Features**:
- Transforms ~834 occupations → ~5,000+ job title rows
- Handles NULL values in Work Experience column
- Uses composite key (OCCUPATION_CODE + JOB_TITLE) for MERGE

**Output Table**: `USER_DB_HYENA.RAW.BLS_EMPLOYMENT_PROJECTIONS`

### 2. Adzuna Job Listings ETL

**Purpose**: Extract job market data for skill demand analysis

**Schedule**: Daily at 2 AM UTC (`0 2 * * *`)

**Tasks**:
1. Create Snowflake table schema
2. Extract: Fetch jobs from Adzuna API (30 categories, 15 pages each)
3. Transform: Clean HTML, normalize salaries, parse locations
4. Load: Batch load with staging + MERGE pattern
5. Data quality checks

**Key Features**:
- Extracts ~22,500 jobs per run (30 categories × 15 pages × 50 jobs)
- Handles rate limiting and retries
- Creates analytics views for skill demand index

**Output Table**: `USER_DB_HYENA.RAW.JOB_LISTINGS`

### 3. College Scorecard ETL

**Purpose**: Extract college tuition, debt, and earnings data

**Schedule**: Daily (`@daily`)

**Tasks**:
1. Create Snowflake table schema
2. Extract: Fetch data from College Scorecard API (pagination)
3. Transform: Flatten nested JSON, handle completion rates
4. Load: Batch load with staging + MERGE pattern
5. Data quality checks

**Key Features**:
- Handles API rate limiting with exponential backoff
- Processes up to 500 pages (~50,000 institutions)
- Handles 4yr vs less-than-4yr completion rates

**Output Table**: `USER_DB_HYENA.RAW.COLLEGE_SCORECARD_DATA`

### 4. WARN ETL

**Purpose**: Extract layoff and closure events for job market analysis

**Schedule**: Monthly (`@monthly`)

**Tasks**:
1. Create Snowflake table schema
2. Extract & Transform: Read CSV, clean and validate data
3. Load: Batch load with staging + MERGE pattern
4. Data quality checks

**Output Table**: `USER_DB_HYENA.RAW.WARN_EVENTS`

## 🔄 dbt Transformations

### Overview

The dbt project transforms raw data from multiple sources into a unified analytical mart that combines college ROI metrics with labor market intelligence. The project standardizes data across different sources using a 25-occupation-group taxonomy to enable cross-source analysis.

### Project Structure

```
dbt/
├── models/
│   ├── marts/
│   │   ├── mart_degree_roi_and_industry_outlook.sql  # Final analytical mart
│   │   ├── schema.yml                                # Mart documentation
│   │   └── staging/
│   │       ├── stg_institution.sql                   # Institution data staging
│   │       ├── stg_jobs.sql                          # Job listings staging
│   │       ├── stg_warn.sql                          # WARN layoffs staging
│   │       ├── stg_bls.sql                          # BLS projections staging
│   │       ├── stg_lookups.sql                      # Unified lookup staging
│   │       └── schema.yml                            # Staging documentation
│   └── sources.yml                                   # Source table definitions
├── macros/
│   └── state_full_name.sql                          # State abbreviation to full name
├── snapshots/
│   └── institution_outcomes.sql                     # Tracks institution data changes
├── seeds/                                            # Static reference data (empty)
├── tests/                                            # Custom tests (empty)
├── dbt_project.yml                                   # Project configuration
└── profiles.yml                                      # Connection profiles
```

### Data Sources

The dbt project integrates data from five source systems (all in `USER_DB_HYENA.RAW`):

1. **Institution Source** (`institution_src`)
   - **Table**: `COLLEGE_SCORECARD_DATA`
   - **Description**: College Scorecard data containing institution information, tuition, earnings, debt, and completion rates

2. **Jobs Source** (`jobs_src`)
   - **Table**: `JOB_LISTINGS`
   - **Description**: Job postings from Adzuna with salary, location, and category information

3. **WARN Source** (`warn_src`)
   - **Table**: `WARN_EVENTS`
   - **Description**: Worker Adjustment and Retraining Notification (WARN) layoff events

4. **BLS Source** (`bls_src`)
   - **Table**: `BLS_EMPLOYMENT_PROJECTIONS`
   - **Description**: Bureau of Labor Statistics employment projections and wage data

5. **Lookups Source** (`lookups_src`)
   - **Tables**: 
     - `LOOKUP_JOB_CATEGORIES` - Maps job categories to occupation groups
     - `LOOKUP_WARN_INDUSTRIES` - Maps WARN industries to occupation groups
     - `LOOKUP_BLS_OCCUPATIONS` - Maps BLS occupations to occupation groups

### Staging Layer (`RAW` Schema)

All staging models are materialized as views and perform:
- Data type conversion and cleaning
- Standardization of values
- Enrichment with occupation group mappings
- Calculation of derived metrics

#### `stg_institution`
- Cleans institution data from College Scorecard
- Converts state abbreviations to full names using `state_full_name` macro
- Calculates ROI metrics:
  - `ROI_IN_STATE`: Earnings (10 years) - In-state tuition
  - `ROI_OUT_OF_STATE`: Earnings (10 years) - Out-of-state tuition
  - `DEBT_TO_EARNINGS_RATIO`: Median debt / Earnings (10 years)
  - `VALUE_INDEX`: Earnings (10 years) - Average net price

#### `stg_jobs`
- Cleans job listing data from Adzuna
- Converts salary strings to numeric values
- Maps job categories to occupation groups via lookup table

#### `stg_warn`
- Cleans WARN layoff event data
- Converts worker count strings to numeric values
- Maps industries to occupation groups via lookup table

#### `stg_bls`
- Cleans BLS employment projection data
- Converts employment numbers and percentages to numeric values
- Maps occupation titles to occupation groups via lookup table

#### `stg_lookups`
- Unified view combining all lookup tables
- Provides a single source for category/industry/occupation to occupation group mappings

### Mart Layer (`ANALYTICS` Schema)

#### `mart_degree_roi_and_industry_outlook`
- **Materialization**: Table
- **Purpose**: Final analytical mart combining all data sources
- **Key Features**:
  - Creates a cross-join of all institutions × all occupation groups
  - Aggregates job market metrics by occupation group (national level)
  - Aggregates layoff metrics by occupation group
  - Aggregates BLS projections by occupation group
  - Calculates risk ratios (layoff-to-posting ratio)

**Key Metrics**:
- Institution ROI metrics (in-state and out-of-state)
- Job posting counts and average salaries by occupation group
- Layoff counts and risk ratios by occupation group
- BLS employment projections (2024-2034) and median wages
- Debt-to-earnings ratios and value indices

### Macros

#### `state_full_name`
Converts US state abbreviations (e.g., 'CA') to full state names (e.g., 'California'). Used in `stg_institution` to standardize state names.

### Snapshots

#### `institution_outcomes_snapshot`
- **Schema**: `SNAPSHOTS`
- **Strategy**: Timestamp-based
- **Unique Key**: `INSTITUTION_ID`
- **Updated At**: `LOAD_TIMESTAMP`
- **Purpose**: Tracks historical changes to institution data over time

### dbt Usage

#### Install Dependencies
```bash
cd dbt
dbt deps
```

#### Run All Models
```bash
dbt run
```

#### Run Specific Models
```bash
# Run only staging models
dbt run --select staging

# Run only the mart
dbt run --select marts

# Run a specific model
dbt run --select stg_institution
```

#### Run Tests
```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --select stg_institution
```

#### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

#### Run Snapshots
```bash
dbt snapshot
```

#### Full Refresh
```bash
# Rebuild all models from scratch
dbt run --full-refresh
```

## 🔧 Configuration

### Snowflake Settings

All pipelines use:
- **Database**: `USER_DB_HYENA`
- **RAW Schema**: For raw data from ETL pipelines
- **ANALYTICS Schema**: For dbt mart models
- **SNAPSHOTS Schema**: For dbt snapshots
- **Connection ID**: `snowflake_conn` (for Airflow)

### Common ETL Patterns

All ETL pipelines use:
- **Staging tables**: Temporary tables for batch loading
- **MERGE operations**: Upsert pattern (update if exists, insert if new)
- **Batch processing**: 1000 rows per batch
- **Transaction control**: BEGIN/COMMIT/ROLLBACK
- **Error handling**: Automatic rollback on failure

### Schema Configuration

- **Staging Models**: `RAW` schema (views)
- **Mart Models**: `ANALYTICS` schema (tables)
- **Snapshots**: `SNAPSHOTS` schema (tables)

## 📈 Complete Data Flow

```
Raw Data Sources (CSV, APIs)
    ↓
[ETL Pipelines - Apache Airflow]
    ↓
Snowflake RAW Schema
    ├── COLLEGE_SCORECARD_DATA
    ├── JOB_LISTINGS
    ├── WARN_EVENTS
    ├── BLS_EMPLOYMENT_PROJECTIONS
    └── Lookup Tables
    ↓
[dbt Transformations]
    ↓
Staging Views (RAW Schema)
    ├── stg_institution
    ├── stg_jobs
    ├── stg_warn
    ├── stg_bls
    └── stg_lookups
    ↓
Analytical Mart (ANALYTICS Schema)
    └── mart_degree_roi_and_industry_outlook
    ↓
[Analytics & Reporting]
    └── Frontend Application ("The Last Degree")
```

## 🗂️ Project Structure

```
Group Project/
├── dags/                                    # ETL Pipelines
│   ├── bls_etl_dag.py                      # BLS ETL pipeline
│   ├── adzuna_etl_dag.py                  # Adzuna ETL pipeline
│   ├── college_scorecard_etl.py            # College Scorecard ETL pipeline
│   ├── warn_etl.py                         # WARN ETL pipeline
│   ├── dbt_elt_dag.py                      # DBT pipeline orchestrator
│   ├── config/
│   │   └── snowflake_config.py            # Snowflake configuration
│   ├── utils/
│   │   └── bls_transformer.py             # BLS transformation logic
│   ├── data/
│   │   ├── Employment Projections.csv
│   │   ├── warn_events.csv
│   │   └── transformed/
│   └── README.md
├── dbt/                                     # dbt Transformations
│   ├── models/
│   │   ├── marts/
│   │   │   ├── mart_degree_roi_and_industry_outlook.sql
│   │   │   ├── schema.yml
│   │   │   └── staging/
│   │   │       ├── stg_institution.sql
│   │   │       ├── stg_jobs.sql
│   │   │       ├── stg_warn.sql
│   │   │       ├── stg_bls.sql
│   │   │       ├── stg_lookups.sql
│   │   │       └── schema.yml
│   │   └── sources.yml
│   ├── macros/
│   │   └── state_full_name.sql
│   ├── snapshots/
│   │   └── institution_outcomes.sql
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── README.md
├── sql/
│   └── create_job_listings_table.sql       # Table creation scripts
├── docker-compose.yaml                      # Docker Airflow setup
├── Dockerfile                               # Custom Airflow image
└── The Last Degree/                         # Frontend application
```

## 🐛 Troubleshooting

### ETL Pipeline Issues

**Docker containers not starting**
```bash
docker-compose down
docker-compose up -d
```

**Airflow not accessible**
- Check if port 8081 is available
- Verify containers are running: `docker-compose ps`

**Snowflake connection errors**
- Verify connection ID is `snowflake_conn`
- Check credentials in Airflow UI
- Ensure warehouse is running

**Import errors**
```bash
docker-compose restart airflow
```

**Missing data files**
- Ensure CSV files are in `dags/data/` directory
- Check Docker volume mounts in `docker-compose.yaml`

### dbt Issues

**Connection errors**
- Verify environment variables are set correctly
- Run `dbt debug` to test connection
- Check `profiles.yml` configuration

**Model build failures**
- Check source table existence: `SELECT * FROM USER_DB_HYENA.RAW.<table_name> LIMIT 1`
- Verify lookup tables are populated
- Review dbt logs for specific error messages

**Test failures**
- Review test output for specific column/model issues
- Check for NULL values in required fields
- Verify data quality in source tables

### Viewing Logs

**Airflow logs**
```bash
# Airflow logs
docker-compose logs airflow

# Specific DAG run
# Navigate to Airflow UI → DAGs → Select DAG → Click on task → View logs
```

**dbt logs**
```bash
cd dbt
dbt run --log-level debug
```

## 📊 Monitoring

### Airflow UI Metrics
- DAG run success rate
- Task execution times
- XCom values (row counts, file paths)
- Historical run data

### Snowflake Queries

```sql
-- Check ETL data
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.BLS_EMPLOYMENT_PROJECTIONS;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.JOB_LISTINGS;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.COLLEGE_SCORECARD_DATA;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.WARN_EVENTS;

-- Check dbt staging models
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.stg_institution;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.stg_jobs;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.stg_warn;
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.stg_bls;

-- Check dbt mart
SELECT COUNT(*) FROM USER_DB_HYENA.ANALYTICS.mart_degree_roi_and_industry_outlook;
```

### dbt Documentation

Generate and view documentation:
```bash
cd dbt
dbt docs generate
dbt docs serve
```

## 🔄 Maintenance

### Daily
- Monitor DAG executions in Airflow UI
- Check for failed tasks
- Verify data loads
- Run dbt transformations after ETL completes

### Monthly
- Update BLS CSV file when new data available
- Update WARN CSV file
- Review and optimize Snowflake queries
- Run dbt snapshots to track changes
- Review test results

### Quarterly
- Dependency updates (Airflow, dbt, Python packages)
- Performance optimization
- Schema evolution planning
- Documentation updates

## 📚 Additional Resources

- [BLS Employment Projections](https://www.bls.gov/emp/)
- [Adzuna API Documentation](https://developer.adzuna.com/)
- [College Scorecard API](https://collegescorecard.ed.gov/data/documentation/)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Snowflake Docs](https://docs.snowflake.com/)
- [dbt Documentation](https://docs.getdbt.com/)

## 🎯 Project Goals

- Provide employment projection data for ROI analysis
- Enable degree-to-job mapping for relevance scoring
- Support 10-year employment forecasting
- Integrate with Graduate ROI Intelligence System
- Standardize data across sources using occupation group taxonomy
- Create unified analytical marts for reporting and visualization

## Occupation Group Taxonomy

All data sources are standardized to one of 25 occupation groups through lookup tables. This enables:
- Cross-source analysis (jobs, layoffs, projections)
- Consistent reporting across different data formats
- Unified analytical mart structure

## Testing

### ETL Testing
- Data quality checks in each pipeline
- Row count validation
- Schema validation

### dbt Testing

The project includes automated tests defined in `schema.yml` files:
- **Not null tests**: Ensure required fields are populated
- **Unique tests**: Ensure primary keys are unique
- **Accepted values tests**: Validate lookup types

Run tests:
```bash
cd dbt
dbt test
```

---

**Version**: 2.0.0  
**Author**: Manav Patel  
**Project**: Graduate ROI Intelligence System  
**Last Updated**: December 2025
