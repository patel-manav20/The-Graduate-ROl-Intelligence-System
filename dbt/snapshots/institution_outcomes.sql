{% snapshot institution_outcomes_snapshot %}

{{
  config(
    target_schema='SNAPSHOTS',
    unique_key='INSTITUTION_ID',
    strategy='timestamp',
    updated_at='LOAD_TIMESTAMP'
  )
}}

select
  INSTITUTION_ID,
  INSTITUTION_NAME,
  STATE,
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
  LOAD_TIMESTAMP
from {{ source('institution_src', 'institution_outcomes') }}

{% endsnapshot %}
