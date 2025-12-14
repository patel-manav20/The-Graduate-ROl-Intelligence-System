# Graduate ROI Intelligence System - ETL Pipelines

A comprehensive Apache Airflow-based ETL system that extracts, transforms, and loads data from multiple sources into Snowflake for ROI analysis and career intelligence.

## 📋 Overview

This project contains 4 ETL pipelines that process data for graduate ROI forecasting:

| Pipeline | Data Source | Schedule | Schema | Table |
|----------|-------------|----------|--------|-------|
| **BLS ETL** | BLS Employment Projections CSV | Monthly | RAW | BLS_EMPLOYMENT_PROJECTIONS |
| **Adzuna ETL** | Adzuna Job API | Daily (2 AM UTC) | RAW | JOB_LISTINGS |
| **College Scorecard ETL** | Dept. of Education API | Daily | RAW | COLLEGE_SCORECARD_DATA |
| **WARN ETL** | WARN Events CSV | Monthly | RAW | WARN_EVENTS |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  BLS CSV    │     │ Adzuna API  │     │  College    │     │  WARN CSV   │
│             │     │             │     │  Scorecard  │     │             │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Docker)                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ BLS DAG  │  │ Adzuna   │  │ College  │  │ WARN DAG │           │
│  │          │  │ DAG      │  │ DAG      │  │          │           │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘           │
└───────┼─────────────┼─────────────┼─────────────┼─────────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   Snowflake Warehouse  │
              │  USER_DB_HYENA.RAW     │
              └────────────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Snowflake account
- API keys (for Adzuna and College Scorecard)

### Installation

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
   - **Account**: ****
   - **User**: HYENA
   - **Password**: ****
   - **Warehouse**: HYENA_QUERRY_WH
   - **Database**: USER_DB_HYENA
   - **Schema**: RAW
   - **Role**: TRAINING_ROLE

6. **Configure Airflow Variables**

   Navigate to Admin → Variables → Add:
   
   | Key | Value |
   |-----|-------|
   | `Adzuna_App_Id` | Your Adzuna App ID |
   | `Adzuna_App_Key` | Your Adzuna App Key |
   | `Adzuna_Pages_Per_Category` | 15 |
   | `COLLEGE_SCORECARD_API_KEY` | (Optional) Your API key |
   | `Employment_Projections_API_Key` | (Optional) Your API key |
   | `Warn_Events_API_Key` | (Optional) Your API key |

8. **Enable DAGs**
   - Open Airflow UI
   - Toggle ON the DAGs you want to run:
     - `bls_employment_projections_etl`
     - `adzuna_job_listings_etl`
     - `college_scorecard_etl`
     - `warn_etl`

## Pipeline Details

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

## Project Structure

```
dags/
├── bls_etl_dag.py                    # BLS ETL pipeline
├── adzuna_etl_dag.py                 # Adzuna ETL pipeline
├── college_scorecard_etl.py          # College Scorecard ETL pipeline
├── warn_etl.py                       # WARN ETL pipeline
├── config/
│   └── snowflake_config.py           # Snowflake configuration
├── utils/
│   └── bls_transformer.py            # BLS transformation logic
├── data/
│   ├── Employment Projections.csv
│   ├── warn_events.csv
│   └── transformed/
└── README.md                          # This file

docker-compose.yaml                   # Docker Airflow setup
```

## Configuration

### Snowflake Settings

All pipelines use:
- **Database**: `USER_DB_HYENA`
- **Schema**: `RAW` (for raw data)
- **Connection ID**: `snowflake_conn`

### Common Patterns

All pipelines use:
- **Staging tables**: Temporary tables for batch loading
- **MERGE operations**: Upsert pattern (update if exists, insert if new)
- **Batch processing**: 1000 rows per batch
- **Transaction control**: BEGIN/COMMIT/ROLLBACK
- **Error handling**: Automatic rollback on failure

## Data Flow

```
Raw Data Sources
    ↓
[Extract & Transform]
    ↓
Transformed Data
    ↓
[Load to Staging Table]
    ↓
[MERGE to Main Table]
    ↓
Snowflake RAW Schema
    ↓
[Analytics & Reporting]
```

## Troubleshooting

### Common Issues

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

### Viewing Logs

```bash
# Airflow logs
docker-compose logs airflow

# Specific DAG run
# Navigate to Airflow UI → DAGs → Select DAG → Click on task → View logs
```

## Monitoring

### Airflow UI Metrics
- DAG run success rate
- Task execution times
- XCom values (row counts, file paths)
- Historical run data

### Snowflake Queries

```sql
-- Check BLS data
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.BLS_EMPLOYMENT_PROJECTIONS;

-- Check Adzuna data
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.JOB_LISTINGS;

-- Check College Scorecard data
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.COLLEGE_SCORECARD_DATA;

-- Check WARN data
SELECT COUNT(*) FROM USER_DB_HYENA.RAW.WARN_EVENTS;
```

## Maintenance

### Daily
- Monitor DAG executions in Airflow UI
- Check for failed tasks
- Verify data loads

### Monthly
- Update BLS CSV file when new data available
- Update WARN CSV file
- Review and optimize Snowflake queries

### Quarterly
- Dependency updates
- Performance optimization
- Schema evolution planning

## Additional Resources

- [BLS Employment Projections](https://www.bls.gov/emp/)
- [Adzuna API Documentation](https://developer.adzuna.com/)
- [College Scorecard API](https://collegescorecard.ed.gov/data/documentation/)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Snowflake Docs](https://docs.snowflake.com/)

## Project Goals

- Provide employment projection data for ROI analysis
- Enable degree-to-job mapping for relevance scoring
- Support 10-year employment forecasting
- Integrate with Graduate ROI Intelligence System

---

**Version**: 2.0.0  
**Author**: Manav Patel  
**Project**: Graduate ROI Intelligence System  
**Last Updated**: December 2025
