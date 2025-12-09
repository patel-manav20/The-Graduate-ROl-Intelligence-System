# BLS Employment Projections ETL Pipeline

An Apache Airflow-based ETL pipeline that extracts BLS (Bureau of Labor Statistics) Employment Projections data from CSV files, transforms it, and loads it into Snowflake.

## 📋 Overview

- **Data Source**: BLS Employment Projections CSV
- **Update Frequency**: Monthly
- **Purpose**: Provides employment projections and wage data for ROI analysis

## 🏗️ Architecture

```
BLS CSV → Extract → Transform → Load to Snowflake
```

### Pipeline Tasks

1. **Create Table**: Creates the target table in Snowflake
2. **Extract & Transform**: Reads CSV, splits occupation titles, cleans data
3. **Load**: Loads transformed data into Snowflake

## 🚀 Setup Instructions

### Prerequisites

- Apache Airflow 2.8+
- Snowflake account
- Docker (for containerized setup)
- BLS Employment Projections CSV file

### Installation

1. **Place CSV file in data directory**

   ```bash
   mkdir -p dags/data/transformed
   cp "Employment Projections.csv" dags/data/Employment\ Projections.csv
   ```

2. **Set up Airflow Variables**

   Navigate to Airflow UI → Admin → Variables:

   | Key | Value |
   |-----|-------|
   | `SNOWFLAKE_DATABASE` | USER_DB_HYENA |
   | `SNOWFLAKE_SCHEMA` | ANALYTICS |
   | `SNOWFLAKE_WAREHOUSE` | HYENA_QUERRY_WH |

3. **Set up Snowflake Connection**

   Navigate to Airflow UI → Admin → Connections:

   - **Connection ID**: `snowflake_conn`
   - **Connection Type**: Snowflake
   - **Account**: SFEDU02-LVB17920
   - **User**: HYENA
   - **Password**: Your Snowflake password
   - **Warehouse**: HYENA_QUERRY_WH
   - **Database**: USER_DB_HYENA
   - **Schema**: ANALYTICS
   - **Role**: TRAINING_ROLE

4. **Start Airflow**

   ```bash
   docker-compose up -d
   ```

5. **Enable DAG**

   - Open Airflow UI at http://localhost:8081
   - Find `bls_employment_projections_etl` DAG
   - Toggle it ON

## 📊 Data Transformation

The pipeline transforms occupation titles that contain multiple job titles:

**Before:**
```
"Accountants and auditors * Accountant * Auditor"
```

**After:**
```
Row 1: Occupation: "Accountants and auditors", Job: "Accountant"
Row 2: Occupation: "Accountants and auditors", Job: "Auditor"
```

**Data Quality:**
- NULL values in "Work Experience" column are replaced with "0"

## 📊 Output Table

**Table**: `USER_DB_HYENA.ANALYTICS.BLS_EMPLOYMENT_PROJECTIONS`

**Key Columns:**
- OCCUPATION_TITLE
- JOB_TITLE
- OCCUPATION_CODE
- EMPLOYMENT_2024, EMPLOYMENT_2034
- MEDIAN_ANNUAL_WAGE_2024
- TYPICAL_ENTRY_LEVEL_EDUCATION
- WORK_EXPERIENCE_IN_RELATED_OCCUPATION

## 🐛 Troubleshooting

### File Not Found
- Ensure CSV file exists at `dags/data/Employment Projections.csv`
- Check Docker volume mounts

### Snowflake Connection Error
- Verify connection ID is `snowflake_conn`
- Check credentials in Airflow UI
- Ensure warehouse is running

### Import Errors
- Restart Airflow: `docker-compose restart airflow`
- Verify all files are in correct directories

## 📁 Project Structure

```
dags/
├── bls_etl_dag.py              # Main DAG
├── config/
│   └── snowflake_config.py     # Configuration
├── utils/
│   └── bls_transformer.py       # Transformation logic
└── data/
    ├── Employment Projections.csv
    └── transformed/
```

## 📝 Monitoring

Monitor pipeline execution in Airflow UI:
- **Graph View**: Task dependencies
- **Logs**: Execution details
- **Tree View**: Historical runs

## 🔗 Resources

- [BLS Employment Projections](https://www.bls.gov/emp/)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Snowflake Docs](https://docs.snowflake.com/)

---

**Version**: 1.0.0  
**Author**: Manav Patel  
**Project**: Graduate ROI Intelligence System
