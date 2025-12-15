{{ config(materialized='view') }}

with src as (
    select
        STATE,
        CITY,
        INDUSTRY,
        NUMBER_OF_WORKERS,
        EFFECTIVE_DATE
    from {{ source('warn_src', 'warn_layoffs') }}
),

typed as (
    select
        STATE,
        CITY,
        INDUSTRY,
        try_to_number(replace(NUMBER_OF_WORKERS, ',', '')) as NUMBER_OF_WORKERS,
        EFFECTIVE_DATE
    from src
),

ind_map as (
    select
        INDUSTRY,
        OCCUPATION_GROUP
    from {{ source('lookups_src', 'lookup_warn_industries') }}
)

select
    w.STATE,
    w.CITY,
    w.INDUSTRY,
    w.NUMBER_OF_WORKERS,
    w.EFFECTIVE_DATE,
    coalesce(m.OCCUPATION_GROUP, 'Other / Unmapped') as OCCUPATION_GROUP
from typed w
left join ind_map m
    on w.INDUSTRY = m.INDUSTRY
