{{ config(
    materialized = 'table',
    schema = 'ANALYTICS'
) }}

with inst as (
    select
        INSTITUTION_ID,
        INSTITUTION_NAME,
        STATE                         as INSTITUTION_STATE,
        ROI_IN_STATE,
        ROI_OUT_OF_STATE,
        DEBT_TO_EARNINGS_RATIO,
        VALUE_INDEX
    from {{ ref('stg_institution') }}
),

-- all occupation groups we care about
occ_dim as (
    select distinct OCCUPATION_GROUP from {{ ref('stg_bls') }}
    union
    select distinct OCCUPATION_GROUP from {{ ref('stg_jobs') }}
    union
    select distinct OCCUPATION_GROUP from {{ ref('stg_warn') }}
),

-- institution × occupation_group grid (no state dependency!)
inst_x_occ as (
    select
        i.INSTITUTION_ID,
        i.INSTITUTION_NAME,
        i.INSTITUTION_STATE,
        o.OCCUPATION_GROUP
    from inst i
    cross join occ_dim o
),

jobs_agg as (
    select
        OCCUPATION_GROUP,
        count(*)                                as JOB_POSTINGS,
        avg(SALARY_AVG)                         as AVG_JOB_SALARY
    from {{ ref('stg_jobs') }}
    group by 1
),

warn_agg as (
    select
        OCCUPATION_GROUP,
        sum(NUMBER_OF_WORKERS)                  as EMPLOYEES_LAID_OFF
    from {{ ref('stg_warn') }}
    group by 1
),

bls_agg as (
    select
        OCCUPATION_GROUP,
        avg(EMPLOYMENT_2024)                    as EMPLOYMENT_2024,
        avg(EMPLOYMENT_2034)                    as EMPLOYMENT_2034,
        avg(EMPLOYMENT_PERCENT_CHANGE_2024_2034) as EMPLOYMENT_PCT_CHANGE,
        avg(MEDIAN_ANNUAL_WAGE_2024)            as MEDIAN_ANNUAL_WAGE_2024
    from {{ ref('stg_bls') }}
    group by 1
)

select
    x.INSTITUTION_ID,
    x.INSTITUTION_NAME,
    x.INSTITUTION_STATE,
    x.OCCUPATION_GROUP,

    -- ROI metrics
    i.ROI_IN_STATE,
    i.ROI_OUT_OF_STATE,
    i.DEBT_TO_EARNINGS_RATIO,
    i.VALUE_INDEX,

    -- labor market metrics (national by occupation_group)
    j.JOB_POSTINGS,
    j.AVG_JOB_SALARY,
    w.EMPLOYEES_LAID_OFF,
    case
        when j.JOB_POSTINGS > 0
            then w.EMPLOYEES_LAID_OFF::float / j.JOB_POSTINGS
    end as LAYOFF_TO_POSTING_RATIO,
    b.EMPLOYMENT_2024,
    b.EMPLOYMENT_2034,
    b.EMPLOYMENT_PCT_CHANGE,
    b.MEDIAN_ANNUAL_WAGE_2024
from inst_x_occ x
left join inst i
  on i.INSTITUTION_ID = x.INSTITUTION_ID
left join jobs_agg j
  on j.OCCUPATION_GROUP = x.OCCUPATION_GROUP
left join warn_agg w
  on w.OCCUPATION_GROUP = x.OCCUPATION_GROUP
left join bls_agg b
  on b.OCCUPATION_GROUP = x.OCCUPATION_GROUP
