{{ config(materialized='view') }}

with src as (
    select
        JOB_ID,
        STATE,
        CITY,
        CATEGORY,
        SALARY_AVG,
        POSTING_DATE,
        LOAD_DATE
    from {{ source('jobs_src', 'job_listings') }}
),

typed as (
    select
        JOB_ID,
        STATE,
        CITY,
        CATEGORY,
        try_to_number(replace(SALARY_AVG, ',', '')) as SALARY_AVG,
        POSTING_DATE,
        LOAD_DATE
    from src
),

cat_map as (
    select
        CATEGORY,
        OCCUPATION_GROUP
    from {{ source('lookups_src', 'lookup_job_categories') }}
)

select
    j.JOB_ID,
    j.STATE,
    j.CITY,
    j.CATEGORY,
    j.SALARY_AVG,
    j.POSTING_DATE,
    j.LOAD_DATE,
    coalesce(c.OCCUPATION_GROUP, 'Other / Unmapped') as OCCUPATION_GROUP
from typed j
left join cat_map c
    on j.CATEGORY = c.CATEGORY
