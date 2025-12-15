{{ config(materialized='view') }}

with src as (
    select
        INSTITUTION_ID,
        INSTITUTION_NAME,
        STATE                as STATE_ABBR,  -- raw (probably 2-letter) code
        CITY,
        ZIP_CODE,
        REGION_ID,
        WEBSITE,
        SCHOOL_TYPE,
        OWNERSHIP,
        STUDENT_SIZE,
        TUITION_IN_STATE,
        TUITION_OUT_OF_STATE,
        AVG_NET_PRICE,
        FEDERAL_GRANTS,
        LOANS_AMOUNT,
        EARNINGS_6YRS,
        EARNINGS_10YRS,
        MEDIAN_DEBT,
        ADMISSION_RATE,
        COMPLETION_RATE,
        LOAD_TIMESTAMP
    from {{ source('institution_src', 'institution_outcomes') }}
),

calc as (
    select
        *,
        case
            when TUITION_IN_STATE is not null and TUITION_IN_STATE > 0
                then (EARNINGS_10YRS - TUITION_IN_STATE)
        end as ROI_IN_STATE,
        case
            when TUITION_OUT_OF_STATE is not null and TUITION_OUT_OF_STATE > 0
                then (EARNINGS_10YRS - TUITION_OUT_OF_STATE)
        end as ROI_OUT_OF_STATE,
        case
            when EARNINGS_10YRS is not null and EARNINGS_10YRS > 0
                then MEDIAN_DEBT / EARNINGS_10YRS::float
        end as DEBT_TO_EARNINGS_RATIO,
        (EARNINGS_10YRS - AVG_NET_PRICE) as VALUE_INDEX
    from src
),

final as (
    select
        INSTITUTION_ID,
        INSTITUTION_NAME,
        {{ state_full_name('STATE_ABBR') }} as STATE,   -- ✅ full state name here
        CITY,
        ZIP_CODE,
        REGION_ID,
        WEBSITE,
        SCHOOL_TYPE,
        OWNERSHIP,
        STUDENT_SIZE,
        ADMISSION_RATE,
        TUITION_IN_STATE,
        TUITION_OUT_OF_STATE,
        AVG_NET_PRICE,
        FEDERAL_GRANTS,
        LOANS_AMOUNT,
        EARNINGS_6YRS,
        EARNINGS_10YRS,
        MEDIAN_DEBT,
        COMPLETION_RATE,
        LOAD_TIMESTAMP,
        ROI_IN_STATE,
        ROI_OUT_OF_STATE,
        DEBT_TO_EARNINGS_RATIO,
        VALUE_INDEX
    from calc
)

select *
from final
