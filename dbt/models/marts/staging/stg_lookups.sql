{{ config(materialized='view') }}

select
    'CATEGORY' as LOOKUP_TYPE,
    CATEGORY as RAW_VALUE,
    OCCUPATION_GROUP
from {{ source('lookups_src', 'lookup_job_categories') }}

union all

select
    'INDUSTRY' as LOOKUP_TYPE,
    INDUSTRY as RAW_VALUE,
    OCCUPATION_GROUP
from {{ source('lookups_src', 'lookup_warn_industries') }}

union all

select
    'OCCUPATION_TITLE' as LOOKUP_TYPE,
    OCCUPATION_TITLE as RAW_VALUE,
    OCCUPATION_GROUP
from {{ source('lookups_src', 'lookup_bls_occupations') }}
