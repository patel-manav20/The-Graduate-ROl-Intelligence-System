# dbt Project Data Flow

This document illustrates the data flow and transformations in the dbt warehouse project.

## Data Flow Diagram

```mermaid
graph TB
    %% Source Layer
    subgraph Sources["Source Tables (RAW Schema)"]
        S1[institution_src<br/>COLLEGE_SCORECARD_DATA<br/>USER_DB_HYENA]
        S2[jobs_src<br/>JOB_LISTINGS<br/>USER_DB_HYENA]
        S3[warn_src<br/>WARN_EVENTS<br/>USER_DB_HYENA]
        S4[bls_src<br/>BLS_EMPLOYMENT_PROJECTIONS<br/>USER_DB_HYENA]
        S5[lookups_src<br/>LOOKUP_JOB_CATEGORIES<br/>LOOKUP_WARN_INDUSTRIES<br/>LOOKUP_BLS_OCCUPATIONS<br/>USER_DB_HYENA]
    end

    %% Staging Layer
    subgraph Staging["Staging Models (RAW Schema - Views)"]
        ST1[stg_institution<br/>• Clean & type data<br/>• Convert state abbrev<br/>• Calculate ROI metrics]
        ST2[stg_jobs<br/>• Clean & type data<br/>• Map category → occupation_group]
        ST3[stg_warn<br/>• Clean & type data<br/>• Map industry → occupation_group]
        ST4[stg_bls<br/>• Clean & type data<br/>• Map occupation → occupation_group]
        ST5[stg_lookups<br/>• Unified lookup view<br/>• All mappings combined]
    end

    %% Macro
    MACRO[state_full_name Macro<br/>Converts state abbreviations<br/>to full names]

    %% Snapshot
    SNAP[institution_outcomes_snapshot<br/>SNAPSHOTS Schema<br/>Tracks historical changes]

    %% Mart Layer
    subgraph Mart["Mart Layer (ANALYTICS Schema)"]
        MART[mart_degree_roi_and_industry_outlook<br/>TABLE<br/>• Institution × Occupation Group<br/>• Aggregated job metrics<br/>• Aggregated layoff metrics<br/>• BLS projections<br/>• Risk ratios]
    end

    %% Connections - Sources to Staging
    S1 --> ST1
    S2 --> ST2
    S3 --> ST3
    S4 --> ST4
    S5 --> ST2
    S5 --> ST3
    S5 --> ST4
    S5 --> ST5

    %% Macro usage
    MACRO -.-> ST1

    %% Snapshot connection
    S1 --> SNAP

    %% Staging to Mart
    ST1 --> MART
    ST2 --> MART
    ST3 --> MART
    ST4 --> MART

    %% Styling
    classDef sourceStyle fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef stagingStyle fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef martStyle fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef macroStyle fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef snapshotStyle fill:#fce4ec,stroke:#880e4f,stroke-width:2px

    class S1,S2,S3,S4,S5 sourceStyle
    class ST1,ST2,ST3,ST4,ST5 stagingStyle
    class MART martStyle
    class MACRO macroStyle
    class SNAP snapshotStyle
```

## Detailed Transformation Flow

### 1. Source to Staging

#### Institution Data Flow
```
COLLEGE_SCORECARD_DATA
  ↓
stg_institution
  ├─ Clean & type conversion
  ├─ Apply state_full_name macro (abbrev → full name)
  ├─ Calculate ROI_IN_STATE
  ├─ Calculate ROI_OUT_OF_STATE
  ├─ Calculate DEBT_TO_EARNINGS_RATIO
  └─ Calculate VALUE_INDEX
```

#### Jobs Data Flow
```
JOB_LISTINGS
  ↓
stg_jobs
  ├─ Clean & type conversion (salary strings → numeric)
  └─ Join with LOOKUP_JOB_CATEGORIES
      └─ Map CATEGORY → OCCUPATION_GROUP
```

#### WARN Data Flow
```
WARN_EVENTS
  ↓
stg_warn
  ├─ Clean & type conversion (worker count strings → numeric)
  └─ Join with LOOKUP_WARN_INDUSTRIES
      └─ Map INDUSTRY → OCCUPATION_GROUP
```

#### BLS Data Flow
```
BLS_EMPLOYMENT_PROJECTIONS
  ↓
stg_bls
  ├─ Clean & type conversion (employment numbers → numeric)
  └─ Join with LOOKUP_BLS_OCCUPATIONS
      └─ Map OCCUPATION_TITLE → OCCUPATION_GROUP
```

### 2. Staging to Mart

```
stg_institution
  ├─ Extract unique institutions
  └─ Create institution dimension
      ↓
stg_jobs + stg_warn + stg_bls
  ├─ Extract unique OCCUPATION_GROUP values
  └─ Create occupation group dimension
      ↓
Cross Join: Institutions × Occupation Groups
  ↓
mart_degree_roi_and_industry_outlook
  ├─ Join institution metrics (ROI, debt, value)
  ├─ Join aggregated job metrics (count, avg salary)
  ├─ Join aggregated layoff metrics (total laid off)
  ├─ Join BLS projections (employment, growth, wages)
  └─ Calculate derived metrics (layoff-to-posting ratio)
```

### 3. Snapshot Process

```
COLLEGE_SCORECARD_DATA
  ↓
institution_outcomes_snapshot
  ├─ Strategy: Timestamp-based
  ├─ Unique Key: INSTITUTION_ID
  ├─ Updated At: LOAD_TIMESTAMP
  └─ Tracks: All institution fields over time
```

## Key Transformations

### Data Standardization
- **Occupation Groups**: All sources mapped to 25 standardized occupation groups
- **State Names**: State abbreviations converted to full names
- **Data Types**: String numbers converted to numeric types
- **Null Handling**: Coalesce operations for unmapped values

### Aggregations
- **Jobs**: Count and average salary by occupation group
- **WARN**: Sum of laid-off workers by occupation group
- **BLS**: Average employment, growth, and wages by occupation group

### Derived Metrics
- **ROI Calculations**: Earnings minus tuition costs
- **Debt Ratios**: Debt relative to earnings
- **Risk Ratios**: Layoffs relative to job postings
- **Value Index**: Earnings minus net price

## Schema Organization

```
RAW Schema (Staging)
├── stg_institution (VIEW)
├── stg_jobs (VIEW)
├── stg_warn (VIEW)
├── stg_bls (VIEW)
└── stg_lookups (VIEW)

ANALYTICS Schema (Marts)
└── mart_degree_roi_and_industry_outlook (TABLE)

SNAPSHOTS Schema
└── institution_outcomes_snapshot (TABLE)
```

## Execution Order

1. **Sources** are loaded by ETL pipelines (Airflow DAGs)
2. **Staging models** run in parallel (no dependencies between them)
3. **Mart model** runs after all staging models complete
4. **Snapshot** runs independently to track changes

## Dependencies

```
stg_institution → mart_degree_roi_and_industry_outlook
stg_jobs → mart_degree_roi_and_industry_outlook
stg_warn → mart_degree_roi_and_industry_outlook
stg_bls → mart_degree_roi_and_industry_outlook

lookups_src → stg_jobs, stg_warn, stg_bls, stg_lookups
state_full_name macro → stg_institution
```

