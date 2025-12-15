{{ config(materialized='view') }}

with src as (
    select
        OCCUPATION_TITLE,
        OCCUPATION_CODE,
        EMPLOYMENT_2024,
        EMPLOYMENT_2034,
        EMPLOYMENT_CHANGE_2024_2034,
        EMPLOYMENT_PERCENT_CHANGE_2024_2034,
        OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG,
        MEDIAN_ANNUAL_WAGE_2024,
        TYPICAL_ENTRY_LEVEL_EDUCATION,
        LOAD_TIMESTAMP
    from {{ source('bls_src', 'bls_occupations') }}
),

typed as (
    select
        OCCUPATION_TITLE,
        OCCUPATION_CODE,
        try_to_number(replace(EMPLOYMENT_2024, ',', ''))                     as EMPLOYMENT_2024,
        try_to_number(replace(EMPLOYMENT_2034, ',', ''))                     as EMPLOYMENT_2034,
        try_to_number(replace(EMPLOYMENT_CHANGE_2024_2034, ',', ''))         as EMPLOYMENT_CHANGE_2024_2034,
        try_to_number(replace(EMPLOYMENT_PERCENT_CHANGE_2024_2034, ',', '')) as EMPLOYMENT_PERCENT_CHANGE_2024_2034,
        try_to_number(replace(OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG, ',', ''))
                                                                             as OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG,
        try_to_number(replace(MEDIAN_ANNUAL_WAGE_2024, ',', ''))             as MEDIAN_ANNUAL_WAGE_2024,
        TYPICAL_ENTRY_LEVEL_EDUCATION,
        LOAD_TIMESTAMP
    from src
),

occ_map as (
    select
        OCCUPATION_TITLE,
        OCCUPATION_GROUP
    from {{ source('lookups_src', 'lookup_bls_occupations') }}
)

select
    b.OCCUPATION_TITLE,
    b.OCCUPATION_CODE,
    b.EMPLOYMENT_2024,
    b.EMPLOYMENT_2034,
    b.EMPLOYMENT_CHANGE_2024_2034,
    b.EMPLOYMENT_PERCENT_CHANGE_2024_2034,
    b.OCCUPATIONAL_OPENINGS_2024_2034_ANNUAL_AVG,
    b.MEDIAN_ANNUAL_WAGE_2024,
    b.TYPICAL_ENTRY_LEVEL_EDUCATION,
    b.LOAD_TIMESTAMP,
    coalesce(m.OCCUPATION_GROUP, 'Other / Unmapped') as OCCUPATION_GROUP
from typed b
left join occ_map m
    on b.OCCUPATION_TITLE = m.OCCUPATION_TITLE
