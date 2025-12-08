"""
Snowflake connection configuration
"""
import os
from airflow.models import Variable

# Snowflake connection parameters
SNOWFLAKE_CONN_ID = 'snowflake_conn'

# Snowflake database and schema 
SNOWFLAKE_DATABASE = Variable.get('SNOWFLAKE_DATABASE', default_var='USER_DB_HYENA')
SNOWFLAKE_SCHEMA = Variable.get('SNOWFLAKE_SCHEMA', default_var='ANALYTICS')
SNOWFLAKE_WAREHOUSE = Variable.get('SNOWFLAKE_WAREHOUSE', default_var='HYENA_QUERRY_WH')

# Table names
BLS_TABLE_NAME = 'bls_employment_projections'

