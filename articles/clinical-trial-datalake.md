# Clinical Trial Data Lake with ducklake

``` r
library(ducklake)
library(dplyr)
library(tidyr)
library(pharmaversesdtm)
library(admiral)
library(lubridate)
library(stringr)
```

## Introduction

While Electronic Data Capture (EDC) systems provide built-in logging and
audit trails for collected clinical trial data, significant data
management challenges emerge once data is exported from the EDC for
statistical programming and analysis. Once SDTM datasets are created and
analysis begins, traditional file-based approaches often result in:

- **Disconnected Flat Files**: CDISC datasets are inherently relational
  (linked by `USUBJID`, domain keys) but are typically stored as
  separate XPT/CSV files or isolated SAS datasets, often requiring
  manual loading and joining for cross-domain analyses
- **Loss of Audit Trail**: EDC protections disappear; tracking changes
  to derived datasets becomes manual
- **Version Control Challenges**: Multiple versions of analysis datasets
  scattered across folders and drives
- **Data Lineage Issues**: Unclear provenance of derived variables and
  ADaM datasets
- **Reproducibility Concerns**: Inability to recreate analyses from
  specific time points
- **Collaboration Friction**: Multiple statistical programmers working
  with different versions of data
- **Regulatory Gaps**: Difficulty demonstrating 21 CFR Part 11
  compliance for derived datasets

The [ducklake](https://tgerke.github.io/ducklake-r/) package addresses
these post-EDC challenges by implementing a versioned data lake
architecture specifically designed for statistical programming workflows
in R. Rather than managing disconnected flat files,
[DuckLake](https://ducklake.select/) provides a modern relational
database structure that preserves the inherent relationships between
CDISC datasets while adding enterprise-grade version control. By storing
SDTM (Study Data Tabulation Model) and ADaM (Analysis Data Model)
datasets along with regulatory submission artifacts (define.xml, ARD,
ARM, specifications) in a DuckLake, statistical programmers gain:

1.  **Relational Data Model**: CDISC datasets are inherently relational
    with explicit keys (`USUBJID`, domain relationships) but
    traditionally stored as disconnected flat files (XPT, CSV). DuckLake
    preserves the relational structure, enabling efficient joins and
    queries across domains without loading multiple files
2.  **Modern Data Architecture**: Move from file-based to
    database-backed workflows while maintaining R’s familiar data frame
    interface
3.  **Automatic Versioning**: Every data modification is tracked with
    timestamps and metadata
4.  **Time Travel**: Query data as it existed at any point in time
5.  **Audit Trail**: Complete history of data changes for regulatory
    compliance
6.  **Reproducibility**: Recreate analyses exactly as they were run
    previously
7.  **Collaboration**: Multiple analysts can work safely with shared
    data
8.  **Performance**: Fast queries on large datasets using DuckDB’s
    columnar storage and query optimization
9.  **Transactions**: Atomic updates ensure data consistency across
    related datasets
10. **Unified Storage**: Keep datasets alongside regulatory artifacts
    (define.xml, ARD, ARM) in one versioned repository

This vignette demonstrates how to set up a clinical trial data lake,
starting with SDTM domains and building through to analysis-ready ADaM
datasets, including storage of regulatory submission artifacts.

## Setting Up the Data Lake

First, we’ll create a new DuckLake to store our clinical trial data.
This establishes the foundational infrastructure for our versioned data
repository.

We’ll use a temporary directory for this vignette, but in practice you
would specify a permanent location using the `lake_path` argument (e.g.,
a shared network drive or project directory).

``` r
# Install the ducklake extension to duckdb (required once per system)
install_ducklake()

# Define where to create a new data lake or access an existing one
# For this vignette, we use tempdir(); in practice, use a permanent location
# lake_path <- "/path/to/your/project/data_lake"
trial_lake_path <- tempdir()

# attach_ducklake creates or attaches (if it already exists) a data lake
attach_ducklake(
  ducklake_name = "clinical_trial_lake",
  lake_path = trial_lake_path
)

# Verify the lake was created
list.files(trial_lake_path, pattern = "clinical_trial_lake")
#> [1] "clinical_trial_lake.ducklake"     "clinical_trial_lake.ducklake.wal"
```

## Data Lake Architecture: Medallion Layers

Before loading data, it’s important to understand the layered
architecture we’ll use. This follows the **medallion architecture**
pattern common in modern data lakes:

- **Bronze Layer (Raw)**: Data exactly as received from source systems,
  with no transformations. This preserves the original data for audit
  trails and reprocessing.
- **Silver Layer (Cleaned)**: Standardized and cleaned data with
  transformations like
  [`convert_blanks_to_na()`](https:/pharmaverse.github.io/admiral/v1.4.1/cran-release/reference/convert_blanks_to_na.html),
  type conversions, and validation. This is the trusted source for
  deriving analysis datasets.
- **Gold Layer (Analytics)**: Business-logic datasets optimized for
  specific analyses, such as ADaM datasets. This is where analysis
  happens.

This approach provides:

1.  **Complete Audit Trail**: Original data is preserved alongside
    transformations
2.  **Reprocessability**: If cleaning logic changes, reprocess from
    bronze without re-extracting
3.  **Data Lineage**: Clear progression from raw → cleaned →
    analysis-ready
4.  **Validation**: Compare layers to verify transformations
5.  **Regulatory Compliance**: Demonstrate no source data was lost or
    improperly altered

## Loading SDTM Domains

SDTM datasets form the foundation of clinical trial data. We’ll load
several key domains from the [pharmaverse SDTM
collection](https://pharmaverse.github.io/pharmaversesdtm/), which
contains realistic test data from the CDISC pilot study.

For each domain, we’ll: 1. Load raw data into the **bronze layer** (as
received) 2. Apply cleaning transformations to create the **silver
layer** (analysis-ready)

### Demographics (DM)

The Demographics domain contains baseline characteristics for each
subject.

``` r
# Bronze layer: Load raw SDTM Demographics exactly as received
with_transaction(
  create_table(pharmaversesdtm::dm, "dm_raw"),
  author = "T Gerke",
  commit_message = "Add raw demographics"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer: Apply cleaning transformations
with_transaction(
  get_ducklake_table("dm_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("dm"),
  author = "T Gerke",
  commit_message = "Clean demographics data"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Verify the cleaned table
get_ducklake_table("dm") |> 
  select(USUBJID, AGE, SEX, RACE, ARM) |> 
  head()
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>   USUBJID       AGE SEX   RACE  ARM                 
#>   <chr>       <dbl> <chr> <chr> <chr>               
#> 1 01-701-1015    63 F     WHITE Placebo             
#> 2 01-701-1023    64 M     WHITE Placebo             
#> 3 01-701-1028    71 M     WHITE Xanomeline High Dose
#> 4 01-701-1033    74 M     WHITE Xanomeline Low Dose 
#> 5 01-701-1034    77 F     WHITE Xanomeline High Dose
#> 6 01-701-1047    85 F     WHITE Placebo
```

### Supplemental Demographics (SUPPDM)

Supplemental domains contain additional variables not in the parent
domain.

``` r
# Bronze layer: Raw data
with_transaction(
  create_table(pharmaversesdtm::suppdm, "suppdm_raw"),
  author = "T Gerke",
  commit_message = "Add raw supplemental demographics"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer: Cleaned data
with_transaction(
  get_ducklake_table("suppdm_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("suppdm"),
  author = "T Gerke",
  commit_message = "Clean supplemental demographics"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Disposition (DS)

The Disposition domain tracks subject progress through the study.

``` r
# Bronze layer
with_transaction(
  create_table(pharmaversesdtm::ds, "ds_raw"),
  author = "T Gerke",
  commit_message = "Add raw disposition"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer
with_transaction(
  ds <- get_ducklake_table("ds_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("ds"),
  author = "T Gerke",
  commit_message = "Clean disposition data"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Exposure (EX)

The Exposure domain contains treatment administration records.

``` r
# Bronze layer
with_transaction(
  create_table(pharmaversesdtm::ex, "ex_raw"),
  author = "T Gerke",
  commit_message = "Add raw exposure"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer
with_transaction(
  ex <- get_ducklake_table("ex_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("ex"),
  author = "T Gerke",
  commit_message = "Clean exposure data"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Adverse Events (AE)

The Adverse Events domain records safety data.

``` r
# Bronze layer
with_transaction(
  create_table(pharmaversesdtm::ae, "ae_raw"),
  author = "T Gerke",
  commit_message = "Add raw adverse events"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer
with_transaction(
  ae <- get_ducklake_table("ae_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("ae"),
  author = "T Gerke",
  commit_message = "Clean adverse events"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Vital Signs (VS)

Vital Signs data will be used for deriving baseline values.

``` r
# Bronze layer
with_transaction(
  create_table(pharmaversesdtm::vs, "vs_raw"),
  author = "T Gerke",
  commit_message = "Add raw vital signs"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer
with_transaction(
  vs <- get_ducklake_table("vs_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("vs"),
  author = "T Gerke",
  commit_message = "Clean vital signs"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Pharmacokinetic Concentrations (PC)

For PK analysis, we’ll also load concentration data.

``` r
# Bronze layer
with_transaction(
  create_table(pharmaversesdtm::pc, "pc_raw"),
  author = "T Gerke",
  commit_message = "Add raw PK concentrations"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Silver layer
with_transaction(
  pc <- get_ducklake_table("pc_raw") |> 
    admiral::convert_blanks_to_na() |> 
    create_table("pc"),
  author = "T Gerke",
  commit_message = "Clean PK concentrations"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Verifying SDTM Version Control

Let’s verify that our SDTM data in the bronze and silver layers is
indeed versioned. Each
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
call within a transaction automatically creates a snapshot with
metadata.

``` r
# View the first 5 of all snapshots in the data lake
list_table_snapshots() |>
  head(5)
#>   snapshot_id       snapshot_time schema_version
#> 1           0 2026-02-06 22:46:35              0
#> 2           1 2026-02-06 22:46:35              1
#> 3           2 2026-02-06 22:46:35              2
#> 4           3 2026-02-06 22:46:35              3
#> 5           4 2026-02-06 22:46:35              4
#>                                                    changes  author
#> 1                                    schemas_created, main    <NA>
#> 2     tables_created, tables_inserted_into, main.dm_raw, 1 T Gerke
#> 3         tables_created, tables_inserted_into, main.dm, 2 T Gerke
#> 4 tables_created, tables_inserted_into, main.suppdm_raw, 3 T Gerke
#> 5     tables_created, tables_inserted_into, main.suppdm, 4 T Gerke
#>                      commit_message commit_extra_info
#> 1                              <NA>              <NA>
#> 2              Add raw demographics              <NA>
#> 3           Clean demographics data              <NA>
#> 4 Add raw supplemental demographics              <NA>
#> 5   Clean supplemental demographics              <NA>

# Filter snapshots for specific tables
list_table_snapshots("dm_raw")
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-02-06 22:46:35              1
#>                                                changes  author
#> 2 tables_created, tables_inserted_into, main.dm_raw, 1 T Gerke
#>         commit_message commit_extra_info
#> 2 Add raw demographics              <NA>
list_table_snapshots("dm")
#>   snapshot_id       snapshot_time schema_version
#> 3           2 2026-02-06 22:46:35              2
#>                                            changes  author
#> 3 tables_created, tables_inserted_into, main.dm, 2 T Gerke
#>            commit_message commit_extra_info
#> 3 Clean demographics data              <NA>
```

This demonstrates that:

1.  **Every table creation is versioned** - Both bronze (`dm_raw`) and
    silver (`dm`) layers have version 1.0
2.  **Metadata is captured** - Each snapshot includes timestamp and
    table information
3.  **Time travel works** - We can retrieve any specific version using
    [`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
4.  **Audit trail exists** - Complete history is maintained for
    regulatory compliance

## Building the Analytics Layer: ADaM Datasets (Gold Layer)

Now that our SDTM data is loaded and versioned in the **silver layer**,
we’ll create analysis datasets following ADaM standards for the **gold
layer**. These datasets apply business logic and derivations optimized
for specific analyses. Each dataset will be stored in the data lake with
full version control.

The gold layer reads from the silver layer (cleaned SDTM), ensuring all
analysis datasets are built from trusted, standardized source data.

### ADSL: Subject-Level Analysis Dataset

ADSL is the fundamental ADaM dataset containing one record per subject
with key analysis variables.

``` r
# Read SDTM data from the lake and collect into memory
# Admiral functions require tibbles/data.frames, not lazy database connections
dm <- get_ducklake_table("dm") |> collect()
suppdm <- get_ducklake_table("suppdm") |> collect()
ds <- get_ducklake_table("ds") |> collect()
ex <- get_ducklake_table("ex") |> collect()
ae <- get_ducklake_table("ae") |> collect()
vs <- get_ducklake_table("vs") |> collect()

# Combine DM and SUPPDM
dm_suppdm <- dm |> 
  left_join(
    suppdm |> 
      filter(QNAM %in% c("EDUCLVL", "DISCONFL", "DSRAEFL")) |> 
      pivot_wider(
        id_cols = c(STUDYID, USUBJID),
        names_from = QNAM,
        values_from = QVAL
      ),
    by = c("STUDYID", "USUBJID")
  )

# Derive treatment dates and durations
ex_ext <- ex |> 
  derive_vars_dtm(
    dtc = EXSTDTC,
    new_vars_prefix = "EXST"
  ) |> 
  derive_vars_dtm(
    dtc = EXENDTC,
    new_vars_prefix = "EXEN",
    time_imputation = "last"
  )

# Derive disposition variables first (needed for later derivations)
ds_ext <- ds |> 
  derive_vars_dt(
    dtc = DSSTDTC,
    new_vars_prefix = "DSST"
  )

# Build ADSL with all derivations in a single pipeline
adsl <- dm_suppdm |> 
  # Treatment Start Datetime
  derive_vars_merged(
    dataset_add = ex_ext,
    filter_add = (EXDOSE > 0 | (EXDOSE == 0 & str_detect(EXTRT, "PLACEBO"))) & 
                 !is.na(EXSTDTM),
    new_vars = exprs(TRTSDTM = EXSTDTM),
    order = exprs(EXSTDTM, EXSEQ),
    mode = "first",
    by_vars = exprs(STUDYID, USUBJID)
  ) |> 
  # Treatment End Datetime
  derive_vars_merged(
    dataset_add = ex_ext,
    filter_add = (EXDOSE > 0 | (EXDOSE == 0 & str_detect(EXTRT, "PLACEBO"))) & 
                 !is.na(EXENDTM),
    new_vars = exprs(TRTEDTM = EXENDTM),
    order = exprs(EXENDTM, EXSEQ),
    mode = "last",
    by_vars = exprs(STUDYID, USUBJID)
  ) |> 
  # Convert to dates
  derive_vars_dtm_to_dt(source_vars = exprs(TRTSDTM, TRTEDTM)) |> 
  # Treatment duration
  derive_var_trtdurd() |> 
  # Safety population flag
  derive_var_merged_exist_flag(
    dataset_add = ex,
    by_vars = exprs(STUDYID, USUBJID),
    new_var = SAFFL,
    condition = (EXDOSE > 0 | (EXDOSE == 0 & str_detect(EXTRT, "PLACEBO")))
  ) |> 
  # Treatment variables
  mutate(
    TRT01P = ARM,
    TRT01A = ACTARM
  ) |> 
  # Age groups
  mutate(
    AGEGR1 = case_when(
      AGE < 18 ~ "<18",
      between(AGE, 18, 64) ~ "18-64",
      AGE > 64 ~ ">64",
      TRUE ~ "Missing"
    ),
    AGEGR1N = case_when(
      AGE < 18 ~ 1,
      between(AGE, 18, 64) ~ 2,
      AGE > 64 ~ 3,
      TRUE ~ 4
    )
  ) |> 
  # Randomization date
  derive_vars_merged(
    dataset_add = ds_ext,
    by_vars = exprs(STUDYID, USUBJID),
    new_vars = exprs(RANDDT = DSSTDT),
    filter_add = DSDECOD == "RANDOMIZED"
  ) |> 
  # End of study date
  derive_vars_merged(
    dataset_add = ds_ext,
    by_vars = exprs(STUDYID, USUBJID),
    new_vars = exprs(EOSDT = DSSTDT),
    filter_add = DSCAT == "DISPOSITION EVENT" & DSDECOD != "SCREEN FAILURE"
  ) |> 
  # End of study status
  mutate(
    EOSSTT = case_when(
      is.na(EOSDT) ~ "ONGOING",
      TRUE ~ "COMPLETED"
    )
  )

# Store ADSL in the data lake
with_transaction(
  create_table(adsl, "adsl"),
  author = "T Gerke",
  commit_message = "Create ADSL dataset",
  commit_extra_info = "Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Preview ADSL
get_ducklake_table("adsl") |> 
  select(USUBJID, AGE, AGEGR1, TRT01P, TRTSDT, TRTEDT, SAFFL) |>
  head(10)
#> # Source:   SQL [?? x 7]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>    USUBJID       AGE AGEGR1 TRT01P               TRTSDT     TRTEDT     SAFFL
#>    <chr>       <dbl> <chr>  <chr>                <date>     <date>     <chr>
#>  1 01-701-1015    63 18-64  Placebo              2014-01-02 2014-07-02 Y    
#>  2 01-701-1023    64 18-64  Placebo              2012-08-05 2012-09-01 Y    
#>  3 01-701-1028    71 >64    Xanomeline High Dose 2013-07-19 2014-01-14 Y    
#>  4 01-701-1033    74 >64    Xanomeline Low Dose  2014-03-18 2014-03-31 Y    
#>  5 01-701-1034    77 >64    Xanomeline High Dose 2014-07-01 2014-12-30 Y    
#>  6 01-701-1047    85 >64    Placebo              2013-02-12 2013-03-09 Y    
#>  7 01-701-1057    59 18-64  Screen Failure       NA         NA         NA   
#>  8 01-701-1097    68 >64    Xanomeline Low Dose  2014-01-01 2014-07-09 Y    
#>  9 01-701-1111    81 >64    Xanomeline Low Dose  2012-09-07 2012-09-16 Y    
#> 10 01-701-1115    84 >64    Xanomeline Low Dose  2012-11-30 2013-01-23 Y
```

### ADAE: Adverse Events Analysis Dataset

ADAE provides analysis-ready adverse event data with treatment-emergent
flags and severity grades.

``` r
# Read ADSL for merging
adsl <- get_ducklake_table("adsl") |> collect()
ae <- get_ducklake_table("ae") |> collect()

# Build ADAE
adae <- ae |>
  # Merge ADSL variables
  derive_vars_merged(
    dataset_add = adsl,
    new_vars = exprs(TRTSDT, TRTEDT, TRT01A, TRT01P),
    by_vars = exprs(STUDYID, USUBJID)
  ) |>
  # Derive analysis dates
  derive_vars_dt(
    dtc = AESTDTC,
    new_vars_prefix = "AST"
  ) |>
  derive_vars_dt(
    dtc = AEENDTC,
    new_vars_prefix = "AEN",
    date_imputation = "last"
  ) |>
  # Derive treatment-emergent flag
  mutate(
    TRTEMFL = if_else(
      !is.na(ASTDT) & !is.na(TRTSDT) & ASTDT >= TRTSDT,
      "Y",
      NA_character_
    )
  ) |>
  # Derive analysis variables
  mutate(
    AOCCPFL = if_else(AESEQ == min(AESEQ), "Y", NA_character_),
    AOCC01FL = AOCCPFL
  ) |>
  group_by(USUBJID, AEDECOD) |>
  mutate(
    AOCC01FL = if_else(row_number() == 1, "Y", NA_character_)
  ) |>
  ungroup()

# Store ADAE in the data lake
with_transaction(
  create_table(adae, "adae"),
  author = "T Gerke",
  commit_message = "Create ADAE dataset",
  commit_extra_info = "Includes treatment-emergent flags and occurrence flags"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Preview ADAE
get_ducklake_table("adae") |>
  filter(TRTEMFL == "Y") |>
  select(USUBJID, AEDECOD, ASTDT, AESEV, TRTEMFL) |>
  head(10)
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>    USUBJID     AEDECOD                              ASTDT      AESEV    TRTEMFL
#>    <chr>       <chr>                                <date>     <chr>    <chr>  
#>  1 01-701-1015 APPLICATION SITE ERYTHEMA            2014-01-03 MILD     Y      
#>  2 01-701-1015 APPLICATION SITE PRURITUS            2014-01-03 MILD     Y      
#>  3 01-701-1015 DIARRHOEA                            2014-01-09 MILD     Y      
#>  4 01-701-1023 ATRIOVENTRICULAR BLOCK SECOND DEGREE 2012-08-26 MILD     Y      
#>  5 01-701-1023 ERYTHEMA                             2012-08-07 MILD     Y      
#>  6 01-701-1023 ERYTHEMA                             2012-08-07 MODERATE Y      
#>  7 01-701-1023 ERYTHEMA                             2012-08-07 MILD     Y      
#>  8 01-701-1028 APPLICATION SITE ERYTHEMA            2013-07-21 MILD     Y      
#>  9 01-701-1028 APPLICATION SITE PRURITUS            2013-08-08 MILD     Y      
#> 10 01-701-1034 APPLICATION SITE PRURITUS            2014-08-27 MILD     Y
```

### ADPC: Pharmacokinetic Concentrations Analysis Dataset

ADPC supports non-compartmental analysis by combining PK concentrations
with dosing records.

``` r
# Read required datasets
adsl <- get_ducklake_table("adsl") |> collect()
pc <- get_ducklake_table("pc") |> collect()
ex <- get_ducklake_table("ex") |> collect()
vs <- get_ducklake_table("vs") |> collect()

# Get ADSL variables needed
adsl_vars <- exprs(TRTSDT, TRTSDTM, TRT01P, TRT01A)

# Derive PC dates and times
pc_dates <- pc |>
  derive_vars_merged(
    dataset_add = adsl,
    new_vars = adsl_vars,
    by_vars = exprs(STUDYID, USUBJID)
  ) |>
  derive_vars_dtm(
    new_vars_prefix = "A",
    dtc = PCDTC,
    time_imputation = "00:00:00",
    ignore_seconds_flag = FALSE
  ) |>
  derive_vars_dtm_to_dt(exprs(ADTM)) |>
  derive_vars_dtm_to_tm(exprs(ADTM)) |>
  derive_vars_dy(reference_date = TRTSDT, source_vars = exprs(ADT)) |>
  mutate(
    EVID = 0,
    NFRLT = if_else(PCTPTNUM < 0, 0, PCTPTNUM)
  )

# Process exposure records
ex_dates <- ex |>
  derive_vars_merged(
    dataset_add = adsl,
    new_vars = adsl_vars,
    by_vars = exprs(STUDYID, USUBJID)
  ) |>
  filter(EXDOSE > 0) |>
  derive_vars_dtm(
    new_vars_prefix = "AST",
    dtc = EXSTDTC,
    time_imputation = "00:00:00"
  ) |>
  mutate(
    EVID = 1,
    NFRLT = 24 * VISITDY
  ) |>
  derive_vars_dtm_to_dt(exprs(ASTDTM))

# Combine PC and EX
adpc <- bind_rows(pc_dates, ex_dates) |>
  arrange(STUDYID, USUBJID, ADTM) |>
  mutate(
    PARAMCD = coalesce(PCTESTCD, "DOSE"),
    AVAL = case_when(
      EVID == 1 ~ EXDOSE,
      PCSTRESC == "<BLQ" & NFRLT == 0 ~ 0,
      PCSTRESC == "<BLQ" & NFRLT > 0 ~ 0.5 * PCLLOQ,
      TRUE ~ PCSTRESN
    ),
    PARAM = case_when(
      PARAMCD == "XAN" ~ "Xanomeline Concentration",
      PARAMCD == "DOSE" ~ "Xanomeline Dose"
    )
  )

# Store ADPC in the data lake
with_transaction(
  create_table(adpc, "adpc"),
  author = "T Gerke",
  commit_message = "Create ADPC dataset",
  commit_extra_info = "PK concentrations with dosing records for NCA"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Preview ADPC
get_ducklake_table("adpc") |>
  filter(PARAMCD == "XAN") |>
  select(USUBJID, ADT, PCTPT, AVAL, PARAM) |>
  head(10)
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>    USUBJID     ADT        PCTPT             AVAL PARAM                   
#>    <chr>       <date>     <chr>            <dbl> <chr>                   
#>  1 01-701-1015 2014-01-01 Pre-dose         0     Xanomeline Concentration
#>  2 01-701-1015 2014-01-02 5 Min Post-dose  0.005 Xanomeline Concentration
#>  3 01-701-1015 2014-01-02 30 Min Post-dose 0.005 Xanomeline Concentration
#>  4 01-701-1015 2014-01-02 1h Post-dose     0.005 Xanomeline Concentration
#>  5 01-701-1015 2014-01-02 1.5h Post-dose   0.005 Xanomeline Concentration
#>  6 01-701-1015 2014-01-02 2h Post-dose     0.005 Xanomeline Concentration
#>  7 01-701-1015 2014-01-02 4h Post-dose     0.005 Xanomeline Concentration
#>  8 01-701-1015 2014-01-02 6h Post-dose     0.005 Xanomeline Concentration
#>  9 01-701-1015 2014-01-02 0-6h Post-dose   0.005 Xanomeline Concentration
#> 10 01-701-1015 2014-01-02 8h Post-dose     0.005 Xanomeline Concentration
```

## Storing Regulatory Submission Artifacts

Beyond datasets, regulatory submissions require metadata and
documentation. The data lake can store various types of artifacts
including define.xml files and other structured data:

### Define.xml Metadata

The define.xml file provides dataset and variable-level metadata
required for regulatory submissions. Store it as a versioned artifact:

``` r
# Example: Store define.xml content
# In practice, you might read this from a file generated by your metadata system
define_xml <- '<?xml version="1.0" encoding="UTF-8"?>
<ODM xmlns="http://www.cdisc.org/ns/odm/v1.3">
  <!-- Define.xml content for ADSL, ADAE, ADPC datasets -->
</ODM>'

# Create a table for regulatory documents
regulatory_docs <- tibble(
  doc_type = "define.xml",
  doc_version = "1.0",
  content = define_xml,
  created_date = Sys.Date(),
  description = "Dataset and variable metadata for regulatory submission"
)

with_transaction(
  create_table(regulatory_docs, "regulatory_documents"),
  author = "T Gerke",
  commit_message = "Add define.xml metadata"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

get_ducklake_table("regulatory_documents")
#> # Source:   table<regulatory_documents> [?? x 5]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>   doc_type   doc_version content                        created_date description
#>   <chr>      <chr>       <chr>                          <date>       <chr>      
#> 1 define.xml 1.0         "<?xml version=\"1.0\" encodi… 2026-02-06   Dataset an…
```

### Storing Different Data Types (JSON Example)

While Analysis Results Metadata (ARM) and Analysis Results Data (ARD)
might be stored as separate data tables in practice, this example
demonstrates how to store structured data in JSON format within the data
lake:

``` r
# Example: Store Analysis Results Metadata (ARM)
arm_content <- tibble(
  analysis_id = "DEMO01",
  analysis_name = "Demographics Table",
  dataset_used = "ADSL",
  program_name = "t_demographics.R",
  output_file = "t_demographics.rtf",
  analysis_date = Sys.Date()
)

# Add to or update regulatory documents table
with_transaction(
  get_ducklake_table("regulatory_documents") |>
    collect() |>
    mutate(content = as.character(content)) |>
    bind_rows(
      tibble(
        doc_type = "ARM",
        doc_version = "1.0",
        content = as.character(jsonlite::toJSON(arm_content)),
        created_date = Sys.Date(),
        description = "Analysis Results Metadata"
      )
    ) |>
    distinct(doc_type, doc_version, .keep_all = TRUE) |>
    replace_table("regulatory_documents"),
  author = "T Gerke",
  commit_message = "Add ARM metadata"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Example: Store Analysis Results Data (ARD)
ard_content <- tibble(
  analysis_id = "DEMO01",
  row_type = "header",
  row_label = "Age (years)",
  treatment = c("Placebo", "Xanomeline Low", "Xanomeline High"),
  n = c(86, 84, 84),
  mean = c(75.2, 75.7, 74.4),
  sd = c(8.59, 7.89, 7.89)
)

with_transaction(
  get_ducklake_table("regulatory_documents") |>
    collect() |>
    mutate(content = as.character(content)) |>
    bind_rows(
      tibble(
        doc_type = "ARD",
        doc_version = "1.0",
        content = as.character(jsonlite::toJSON(ard_content)),
        created_date = Sys.Date(),
        description = "Analysis Results Data for Demographics Table"
      )
    ) |>
    distinct(doc_type, doc_version, .keep_all = TRUE) |>
    replace_table("regulatory_documents"),
  author = "T Gerke",
  commit_message = "Add demographics ARD"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Dataset Specifications

Store dataset specifications alongside the data:

``` r
# Example: Store ADSL specifications
adsl_spec <- tibble(
  dataset = "ADSL",
  variable = c("USUBJID", "AGE", "AGEGR1", "TRT01P", "SAFFL"),
  label = c(
    "Unique Subject Identifier",
    "Age",
    "Age Group 1",
    "Planned Treatment",
    "Safety Population Flag"
  ),
  type = c("text", "num", "text", "text", "text"),
  length = c(20, 8, 10, 40, 1),
  derivation = c("DM.USUBJID", "DM.AGE", "Derived from AGE", "DM.ARM", "Derived")
)

with_transaction(
  create_table(adsl_spec, "dataset_specifications"),
  author = "T Gerke",
  commit_message = "Add ADSL specifications"
)

# Query specifications when needed
get_ducklake_table("dataset_specifications") |>
  filter(dataset == "ADSL")
```

This unified approach ensures that all submission artifacts are
version-controlled alongside the datasets they describe, maintaining
perfect alignment between data and documentation.

## Organizational Structure and Cohesion

One of the key advantages of the data lake approach is that it preserves
and leverages the inherently relational structure of CDISC data.

### Relational Structure: Beyond Flat Files

Many clinical trial workflows store each SDTM domain and ADaM dataset as
separate flat files (XPT, SAS7BDAT, CSV). While this meets regulatory
requirements for submission formats, it often loses the relational
structure inherent in CDISC standards during day-to-day analysis work.
Every SDTM domain shares `STUDYID` and `USUBJID` as keys, and domains
are explicitly designed to relate to each other (e.g., EX records link
to DM subjects, AE records link to both DM and EX).

While some organizations use SAS datasets in databases or other database
solutions, DuckLake provides an R-native approach that preserves these
relationships with version control built in:

``` r
# Traditional approach: Load multiple files, manually join
# adsl <- read_xpt("adsl.xpt")
# ex <- read_xpt("ex.xpt")
# ae <- read_xpt("ae.xpt")
# result <- adsl |> left_join(ex, ...) |> left_join(ae, ...)

# DuckLake approach: Query across related tables directly using dplyr
# Complex cross-domain query without loading all data
adsl_tbl <- get_ducklake_table("adsl")
ex_tbl <- get_ducklake_table("ex")
ae_tbl <- get_ducklake_table("ae")

adsl_tbl |>
  filter(SAFFL == "Y") |>
  left_join(ex_tbl, by = "USUBJID") |>
  left_join(ae_tbl, by = "USUBJID") |>
  group_by(USUBJID, AGE, TRT01P) |>
  summarise(
    n_aes = n_distinct(AESEQ, na.rm = TRUE),
    total_dose = sum(EXDOSE, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(desc(n_aes)) |>
  head(10) |>
  collect()
#> # A tibble: 10 × 5
#>    USUBJID       AGE TRT01P               n_aes total_dose
#>    <chr>       <dbl> <chr>                <dbl>      <dbl>
#>  1 01-701-1302    61 Xanomeline High Dose    23       3105
#>  2 01-717-1004    80 Xanomeline Low Dose     19       3078
#>  3 01-704-1266    82 Xanomeline High Dose    16       2160
#>  4 01-709-1029    82 Xanomeline High Dose    16       3024
#>  5 01-718-1427    74 Xanomeline High Dose    16       2160
#>  6 01-701-1275    61 Xanomeline High Dose    15       2025
#>  7 01-713-1179    64 Placebo                 15          0
#>  8 01-701-1192    80 Xanomeline Low Dose     15       2430
#>  9 01-709-1309    65 Xanomeline High Dose    15       2835
#> 10 01-711-1143    76 Xanomeline Low Dose     14       1512
```

### Data Warehouse Benefits

This approach provides traditional data warehouse capabilities for
clinical trials:

``` r
# 1. Single source of truth - all datasets in one repository
# List all tables in the data lake
DBI::dbListTables(duckplyr:::get_default_duckdb_connection())
#>  [1] "adae"                                 
#>  [2] "adpc"                                 
#>  [3] "adsl"                                 
#>  [4] "ae"                                   
#>  [5] "ae_raw"                               
#>  [6] "dm"                                   
#>  [7] "dm_raw"                               
#>  [8] "ds"                                   
#>  [9] "ds_raw"                               
#> [10] "ducklake_column"                      
#> [11] "ducklake_column_mapping"              
#> [12] "ducklake_column_tag"                  
#> [13] "ducklake_data_file"                   
#> [14] "ducklake_delete_file"                 
#> [15] "ducklake_file_column_stats"           
#> [16] "ducklake_file_partition_value"        
#> [17] "ducklake_files_scheduled_for_deletion"
#> [18] "ducklake_inlined_data_tables"         
#> [19] "ducklake_metadata"                    
#> [20] "ducklake_name_mapping"                
#> [21] "ducklake_partition_column"            
#> [22] "ducklake_partition_info"              
#> [23] "ducklake_schema"                      
#> [24] "ducklake_schema_versions"             
#> [25] "ducklake_snapshot"                    
#> [26] "ducklake_snapshot_changes"            
#> [27] "ducklake_table"                       
#> [28] "ducklake_table_column_stats"          
#> [29] "ducklake_table_stats"                 
#> [30] "ducklake_tag"                         
#> [31] "ducklake_view"                        
#> [32] "ex"                                   
#> [33] "ex_raw"                               
#> [34] "pc"                                   
#> [35] "pc_raw"                               
#> [36] "regulatory_documents"                 
#> [37] "suppdm"                               
#> [38] "suppdm_raw"                           
#> [39] "vs"                                   
#> [40] "vs_raw"

# 2. Efficient filtering before loading into R
# Only load subjects with adverse events
get_ducklake_table("ae") |>
  filter(AESEV == "SEVERE") |>
  distinct(USUBJID)
#> # Source:   SQL [?? x 1]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>    USUBJID    
#>    <chr>      
#>  1 01-703-1086
#>  2 01-703-1119
#>  3 01-706-1049
#>  4 01-708-1428
#>  5 01-710-1083
#>  6 01-718-1079
#>  7 01-718-1170
#>  8 01-704-1008
#>  9 01-704-1445
#> 10 01-710-1070
#> # ℹ more rows

# 3. Aggregations performed at database level
get_ducklake_table("adae") |>
  filter(TRTEMFL == "Y") |>
  group_by(TRT01A, AESEV) |>
  summarise(
    n_events = n(),
    n_subjects = n_distinct(USUBJID),
    .groups = "drop"
  )
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>   TRT01A               AESEV    n_events n_subjects
#>   <chr>                <chr>       <dbl>      <dbl>
#> 1 Xanomeline High Dose MILD          287         65
#> 2 Xanomeline Low Dose  MODERATE      170         58
#> 3 Placebo              SEVERE          6          5
#> 4 Xanomeline Low Dose  MILD          232         64
#> 5 Xanomeline High Dose MODERATE      115         46
#> 6 Placebo              MODERATE       65         25
#> 7 Xanomeline Low Dose  SEVERE         25         16
#> 8 Placebo              MILD          210         58
#> 9 Xanomeline High Dose SEVERE         10          8

# 4. Joins across SDTM and ADaM layers
# Example: Find date discrepancies between SDTM and ADaM
ae_sdtm <- get_ducklake_table("ae") |>
  select(USUBJID, AESEQ, ae_date = AESTDTC, ae_term = AEDECOD)

adae_adam <- get_ducklake_table("adae") |>
  select(USUBJID, AESEQ, adae_date = ASTDT, adae_term = AEDECOD)

ae_sdtm |>
  inner_join(adae_adam, by = c("USUBJID", "AESEQ")) |>
  # Convert SDTM character date to comparable format for filtering
  mutate(ae_date_comparable = substr(ae_date, 1, 10)) |>
  filter(ae_date_comparable != as.character(adae_date)) |>
  select(
    USUBJID,
    sdtm_start_date = ae_date,
    adam_start_date = adae_date,
    sdtm_term = ae_term,
    adam_term = adae_term
  )
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#> # ℹ 5 variables: USUBJID <chr>, sdtm_start_date <chr>, adam_start_date <date>,
#> #   sdtm_term <chr>, adam_term <chr>
# Note: This returns 0 rows with clean pharmaversesdtm data,
# but demonstrates how to check for data quality issues across layers
```

### Cohesive Dataset Relationships

Let’s explore how our datasets are connected:

``` r
# List all tables in the data lake
DBI::dbListTables(duckplyr:::get_default_duckdb_connection())
#>  [1] "adae"                                 
#>  [2] "adpc"                                 
#>  [3] "adsl"                                 
#>  [4] "ae"                                   
#>  [5] "ae_raw"                               
#>  [6] "dm"                                   
#>  [7] "dm_raw"                               
#>  [8] "ds"                                   
#>  [9] "ds_raw"                               
#> [10] "ducklake_column"                      
#> [11] "ducklake_column_mapping"              
#> [12] "ducklake_column_tag"                  
#> [13] "ducklake_data_file"                   
#> [14] "ducklake_delete_file"                 
#> [15] "ducklake_file_column_stats"           
#> [16] "ducklake_file_partition_value"        
#> [17] "ducklake_files_scheduled_for_deletion"
#> [18] "ducklake_inlined_data_tables"         
#> [19] "ducklake_metadata"                    
#> [20] "ducklake_name_mapping"                
#> [21] "ducklake_partition_column"            
#> [22] "ducklake_partition_info"              
#> [23] "ducklake_schema"                      
#> [24] "ducklake_schema_versions"             
#> [25] "ducklake_snapshot"                    
#> [26] "ducklake_snapshot_changes"            
#> [27] "ducklake_table"                       
#> [28] "ducklake_table_column_stats"          
#> [29] "ducklake_table_stats"                 
#> [30] "ducklake_tag"                         
#> [31] "ducklake_view"                        
#> [32] "ex"                                   
#> [33] "ex_raw"                               
#> [34] "pc"                                   
#> [35] "pc_raw"                               
#> [36] "regulatory_documents"                 
#> [37] "suppdm"                               
#> [38] "suppdm_raw"                           
#> [39] "vs"                                   
#> [40] "vs_raw"

# View snapshot history for key tables
metadata_tables <- c("dm", "ex", "ae", "pc", 
                     "adsl", "adae", "adpc")

# Collect snapshots for all tables
purrr::map_dfr(metadata_tables, ~{
  list_table_snapshots(.x) |>
    mutate(table = .x, .before = 1)
}) |>
  select(table, snapshot_id, snapshot_time, changes)
#>   table snapshot_id       snapshot_time
#> 1    dm           2 2026-02-06 22:46:35
#> 2    ex           8 2026-02-06 22:46:36
#> 3    ae          10 2026-02-06 22:46:36
#> 4    pc          14 2026-02-06 22:46:36
#> 5  adsl          15 2026-02-06 22:46:37
#> 6  adae          16 2026-02-06 22:46:38
#> 7  adpc          17 2026-02-06 22:46:38
#>                                               changes
#> 1    tables_created, tables_inserted_into, main.dm, 2
#> 2    tables_created, tables_inserted_into, main.ex, 8
#> 3   tables_created, tables_inserted_into, main.ae, 10
#> 4   tables_created, tables_inserted_into, main.pc, 14
#> 5 tables_created, tables_inserted_into, main.adsl, 15
#> 6 tables_created, tables_inserted_into, main.adae, 16
#> 7 tables_created, tables_inserted_into, main.adpc, 17

# Check all ADAE subjects exist in ADSL
adae_tbl <- get_ducklake_table("adae")
adsl_tbl <- get_ducklake_table("adsl")

integrity_check <- adae_tbl |>
  anti_join(adsl_tbl, by = "USUBJID") |>
  summarise(orphaned_records = n()) |>
  collect()

# ADAE records without ADSL subject:
integrity_check |> pull(orphaned_records)
#> [1] 0
```

This relational approach means your clinical trial data lake functions
as a purpose-built data warehouse, designed specifically for the
relational nature of CDISC standards.

## Demonstrating Core Functionality

### Version Control and Snapshots

Every modification to tables is automatically versioned. Let’s
demonstrate by adding new derived variables to ADSL:

``` r
# Add new derived columns using dplyr syntax
# replace_table() handles the DROP/CREATE cycle internally
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(
      AGE65FL = if_else(AGE >= 65, "Y", "N"),
      AGECAT = case_when(
        AGE < 65 ~ "<65",
        AGE >= 65 & AGE < 75 ~ "65-74",
        AGE >= 75 ~ ">=75",
        TRUE ~ NA_character_
      )
    ) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Add age categorization vars"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# View version history - should now show 2 snapshots
list_table_snapshots("adsl")
#>    snapshot_id       snapshot_time schema_version
#> 16          15 2026-02-06 22:46:37             15
#> 22          21 2026-02-06 22:46:40             21
#>                                                                    changes
#> 16                     tables_created, tables_inserted_into, main.adsl, 15
#> 22 tables_created, tables_dropped, tables_inserted_into, main.adsl, 15, 21
#>     author              commit_message
#> 16 T Gerke         Create ADSL dataset
#> 22 T Gerke Add age categorization vars
#>                                                                      commit_extra_info
#> 16 Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups
#> 22                                                                                <NA>

# Verify new columns exist
get_ducklake_table("adsl") |>
  select(USUBJID, AGE, AGE65FL, AGECAT) |>
  head(5)
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>   USUBJID       AGE AGE65FL AGECAT
#>   <chr>       <dbl> <chr>   <chr> 
#> 1 01-701-1015    63 N       <65   
#> 2 01-701-1023    64 N       <65   
#> 3 01-701-1028    71 Y       65-74 
#> 4 01-701-1033    74 Y       65-74 
#> 5 01-701-1034    77 Y       >=75
```

#### Choosing Between replace_table() and update_table()

The ducklake package provides two functions for modifying tables, each
optimized for different use cases:

**Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
when:**

- **Adding or removing columns** - DuckLake UPDATE operations can only
  modify existing column values, not change the schema
- **You want versioning** - Creates a new snapshot via DROP + CREATE,
  enabling time travel to previous schemas
- **Making bulk transformations** - Efficient for applying complex dplyr
  pipelines that restructure data
- **Working with dplyr pipelines** - Provides a natural dplyr-like
  interface:
  `get_ducklake_table(name) |> mutate(...) |> replace_table(name)`

**Use
[`update_table()`](https://tgerke.github.io/ducklake-r/reference/update_table.md)
when:**

- **Non-GxP workflows only** - Data pipelines where regulatory audit
  trails are not required
- **Never for clinical trial submission datasets** - The lack of audit
  trail creates regulatory compliance gaps

**⚠️ Critical for Clinical Trials:**
[`update_table()`](https://tgerke.github.io/ducklake-r/reference/update_table.md)
is **not appropriate for GxP-validated work**. It permanently modifies
tables **without creating snapshots or audit trails**, violating
regulatory requirements (21 CFR Part 11, ICH GCP):

- ❌ Changes cannot be time-traveled back to
- ❌ No record of what values changed or when  
- ❌ Cannot prove data integrity for regulatory inspections
- ❌ Violates requirement for complete data lineage

**Recommendation:** For clinical trial data, default to
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for all modifications, no matter how minor. The audit trail is more
important than avoiding “version clutter.”

**For GxP-compliant workflows, use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for all changes** to ensure complete audit trails and the ability to
recreate previous states.

**Note on versioning:** While transactions
([`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)/[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md))
ensure atomic operations, only CREATE operations trigger snapshot
creation. UPDATE operations modify tables in-place without snapshots,
regardless of transaction context.

**Bottom line for clinical trials:** If a change matters enough to make,
it matters enough to audit. Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
wrapped in a transaction for any data modification - whether correcting
a single value or adding new columns:

``` r
# Correcting a specific value - creates auditable snapshot
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(SAFFL = if_else(USUBJID == "01-701-1015", "N", SAFFL)) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Correct safety flag"
)

# Adding new derived columns - creates auditable snapshot
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(AGE65FL = if_else(AGE >= 65, "Y", "N")) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Add age 65+ flag"
)
```

Both operations create snapshots you can time-travel back to and include
in your regulatory audit trail.

#### Iterative Development with Full Audit Trail

When developing derivations, create a snapshot at each meaningful
iteration to maintain a complete audit trail:

``` r
# Iteration 1: First attempt (creates snapshot v2)
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(AGECAT_TEST = case_when(
      AGE < 50 ~ "Young",
      AGE >= 50 ~ "Older"
    )) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Test age categories v1"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Iteration 2: Refinement (creates snapshot v3)
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(AGECAT_TEST = case_when(
      AGE < 40 ~ "18-39",
      AGE < 65 ~ "40-64",
      AGE >= 65 ~ "65+"
    )) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Refine age categories v2"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Iteration 3: Final version (creates snapshot v4)
with_transaction(
  get_ducklake_table("adsl") |>
    mutate(
      AGECAT_TEST = NULL,
      AGECAT2 = case_when(
        AGE < 40 ~ "18-39",
        AGE < 65 ~ "40-64",
        AGE >= 65 ~ "65+",
        TRUE ~ "Missing"
      )
    ) |>
    replace_table("adsl"),
  author = "T Gerke",
  commit_message = "Finalize age categories"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Complete audit trail available
snapshots <- list_table_snapshots("adsl")
snapshots  # Shows all iterations with snapshot metadata
#>    snapshot_id       snapshot_time schema_version
#> 16          15 2026-02-06 22:46:37             15
#> 22          21 2026-02-06 22:46:40             21
#> 23          22 2026-02-06 22:46:40             22
#> 24          23 2026-02-06 22:46:40             23
#> 25          24 2026-02-06 22:46:40             24
#>                                                                    changes
#> 16                     tables_created, tables_inserted_into, main.adsl, 15
#> 22 tables_created, tables_dropped, tables_inserted_into, main.adsl, 15, 21
#> 23 tables_created, tables_dropped, tables_inserted_into, main.adsl, 21, 22
#> 24 tables_created, tables_dropped, tables_inserted_into, main.adsl, 22, 23
#> 25 tables_created, tables_dropped, tables_inserted_into, main.adsl, 23, 24
#>     author              commit_message
#> 16 T Gerke         Create ADSL dataset
#> 22 T Gerke Add age categorization vars
#> 23 T Gerke      Test age categories v1
#> 24 T Gerke    Refine age categories v2
#> 25 T Gerke     Finalize age categories
#>                                                                      commit_extra_info
#> 16 Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups
#> 22                                                                                <NA>
#> 23                                                                                <NA>
#> 24                                                                                <NA>
#> 25                                                                                <NA>

# Each row represents a point in time you can restore to
# - snapshot_id: Unique identifier for this version
# - snapshot_time: When this version was created
# - changes: What operations created this snapshot

# Time-travel to specific snapshots using snapshot_id
# Use actual snapshot IDs from the list (first, second, and last)
snapshot_ids <- snapshots$snapshot_id
adsl_v1 <- get_ducklake_table_version("adsl", snapshot_ids[1])
adsl_v2 <- get_ducklake_table_version("adsl", snapshot_ids[2])
adsl_final <- get_ducklake_table_version("adsl", snapshot_ids[length(snapshot_ids)])

# Or use snapshot times for time-travel
adsl_asof <- get_ducklake_table_asof("adsl", snapshots$snapshot_time[2])

# Compare columns across iterations
colnames(adsl_v1 |> collect())  # Initial version
#>  [1] "STUDYID"  "DOMAIN"   "USUBJID"  "SUBJID"   "RFSTDTC"  "RFENDTC" 
#>  [7] "RFXSTDTC" "RFXENDTC" "RFICDTC"  "RFPENDTC" "DTHDTC"   "DTHFL"   
#> [13] "SITEID"   "BRTHDTC"  "AGE"      "AGEU"     "SEX"      "RACE"    
#> [19] "ETHNIC"   "ARMCD"    "ARM"      "ACTARMCD" "ACTARM"   "COUNTRY" 
#> [25] "DMDTC"    "DMDY"     "TRTSDTM"  "TRTEDTM"  "TRTSDT"   "TRTEDT"  
#> [31] "TRTDURD"  "SAFFL"    "TRT01P"   "TRT01A"   "AGEGR1"   "AGEGR1N" 
#> [37] "RANDDT"   "EOSDT"    "EOSSTT"
colnames(adsl_final |> collect())  # Final version with all derivations
#>  [1] "STUDYID"  "DOMAIN"   "USUBJID"  "SUBJID"   "RFSTDTC"  "RFENDTC" 
#>  [7] "RFXSTDTC" "RFXENDTC" "RFICDTC"  "RFPENDTC" "DTHDTC"   "DTHFL"   
#> [13] "SITEID"   "BRTHDTC"  "AGE"      "AGEU"     "SEX"      "RACE"    
#> [19] "ETHNIC"   "ARMCD"    "ARM"      "ACTARMCD" "ACTARM"   "COUNTRY" 
#> [25] "DMDTC"    "DMDY"     "TRTSDTM"  "TRTEDTM"  "TRTSDT"   "TRTEDT"  
#> [31] "TRTDURD"  "SAFFL"    "TRT01P"   "TRT01A"   "AGEGR1"   "AGEGR1N" 
#> [37] "RANDDT"   "EOSDT"    "EOSSTT"   "AGE65FL"  "AGECAT"   "AGECAT2"
```

This GxP-compliant approach ensures:

- Complete audit trail of all derivation iterations
- Ability to recreate any intermediate state
- Proof of what changed and when for regulatory inspections
- Data lineage from initial to final derivation

### Time Travel

Query data as it existed at a specific point in time:

``` r
# Get the current version
adsl_current <- get_ducklake_table("adsl")

# Get the version history for adsl
versions <- list_table_snapshots("adsl")
print(versions)
#>    snapshot_id       snapshot_time schema_version
#> 16          15 2026-02-06 22:46:37             15
#> 22          21 2026-02-06 22:46:40             21
#> 23          22 2026-02-06 22:46:40             22
#> 24          23 2026-02-06 22:46:40             23
#> 25          24 2026-02-06 22:46:40             24
#>                                                                    changes
#> 16                     tables_created, tables_inserted_into, main.adsl, 15
#> 22 tables_created, tables_dropped, tables_inserted_into, main.adsl, 15, 21
#> 23 tables_created, tables_dropped, tables_inserted_into, main.adsl, 21, 22
#> 24 tables_created, tables_dropped, tables_inserted_into, main.adsl, 22, 23
#> 25 tables_created, tables_dropped, tables_inserted_into, main.adsl, 23, 24
#>     author              commit_message
#> 16 T Gerke         Create ADSL dataset
#> 22 T Gerke Add age categorization vars
#> 23 T Gerke      Test age categories v1
#> 24 T Gerke    Refine age categories v2
#> 25 T Gerke     Finalize age categories
#>                                                                      commit_extra_info
#> 16 Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups
#> 22                                                                                <NA>
#> 23                                                                                <NA>
#> 24                                                                                <NA>
#> 25                                                                                <NA>

# Get data from the first snapshot version
first_snapshot_id <- versions |>
  slice(1) |>
  pull(snapshot_id)

adsl_v1 <- get_ducklake_table_version(
  table_name = "adsl",
  version = first_snapshot_id
)

# Compare versions - earlier version shouldn't have derived variables added later
adsl_v1 |> collect()
#> # A tibble: 306 × 39
#>    STUDYID      DOMAIN USUBJID  SUBJID RFSTDTC RFENDTC RFXSTDTC RFXENDTC RFICDTC
#>    <chr>        <chr>  <chr>    <chr>  <chr>   <chr>   <chr>    <chr>    <chr>  
#>  1 CDISCPILOT01 DM     01-701-… 1015   2014-0… 2014-0… 2014-01… 2014-07… NA     
#>  2 CDISCPILOT01 DM     01-701-… 1023   2012-0… 2012-0… 2012-08… 2012-09… NA     
#>  3 CDISCPILOT01 DM     01-701-… 1028   2013-0… 2014-0… 2013-07… 2014-01… NA     
#>  4 CDISCPILOT01 DM     01-701-… 1033   2014-0… 2014-0… 2014-03… 2014-03… NA     
#>  5 CDISCPILOT01 DM     01-701-… 1034   2014-0… 2014-1… 2014-07… 2014-12… NA     
#>  6 CDISCPILOT01 DM     01-701-… 1047   2013-0… 2013-0… 2013-02… 2013-03… NA     
#>  7 CDISCPILOT01 DM     01-701-… 1057   NA      NA      NA       NA       NA     
#>  8 CDISCPILOT01 DM     01-701-… 1097   2014-0… 2014-0… 2014-01… 2014-07… NA     
#>  9 CDISCPILOT01 DM     01-701-… 1111   2012-0… 2012-0… 2012-09… 2012-09… NA     
#> 10 CDISCPILOT01 DM     01-701-… 1115   2012-1… 2013-0… 2012-11… 2013-01… NA     
#> # ℹ 296 more rows
#> # ℹ 30 more variables: RFPENDTC <chr>, DTHDTC <chr>, DTHFL <chr>, SITEID <chr>,
#> #   BRTHDTC <chr>, AGE <dbl>, AGEU <chr>, SEX <chr>, RACE <chr>, ETHNIC <chr>,
#> #   ARMCD <chr>, ARM <chr>, ACTARMCD <chr>, ACTARM <chr>, COUNTRY <chr>,
#> #   DMDTC <chr>, DMDY <dbl>, TRTSDTM <dttm>, TRTEDTM <dttm>, TRTSDT <date>,
#> #   TRTEDT <date>, TRTDURD <dbl>, SAFFL <chr>, TRT01P <chr>, TRT01A <chr>,
#> #   AGEGR1 <chr>, AGEGR1N <dbl>, RANDDT <date>, EOSDT <date>, EOSSTT <chr>
adsl_current |> collect()
#> # A tibble: 306 × 42
#>    STUDYID      DOMAIN USUBJID  SUBJID RFSTDTC RFENDTC RFXSTDTC RFXENDTC RFICDTC
#>    <chr>        <chr>  <chr>    <chr>  <chr>   <chr>   <chr>    <chr>    <chr>  
#>  1 CDISCPILOT01 DM     01-701-… 1015   2014-0… 2014-0… 2014-01… 2014-07… NA     
#>  2 CDISCPILOT01 DM     01-701-… 1023   2012-0… 2012-0… 2012-08… 2012-09… NA     
#>  3 CDISCPILOT01 DM     01-701-… 1028   2013-0… 2014-0… 2013-07… 2014-01… NA     
#>  4 CDISCPILOT01 DM     01-701-… 1033   2014-0… 2014-0… 2014-03… 2014-03… NA     
#>  5 CDISCPILOT01 DM     01-701-… 1034   2014-0… 2014-1… 2014-07… 2014-12… NA     
#>  6 CDISCPILOT01 DM     01-701-… 1047   2013-0… 2013-0… 2013-02… 2013-03… NA     
#>  7 CDISCPILOT01 DM     01-701-… 1057   NA      NA      NA       NA       NA     
#>  8 CDISCPILOT01 DM     01-701-… 1097   2014-0… 2014-0… 2014-01… 2014-07… NA     
#>  9 CDISCPILOT01 DM     01-701-… 1111   2012-0… 2012-0… 2012-09… 2012-09… NA     
#> 10 CDISCPILOT01 DM     01-701-… 1115   2012-1… 2013-0… 2012-11… 2013-01… NA     
#> # ℹ 296 more rows
#> # ℹ 33 more variables: RFPENDTC <chr>, DTHDTC <chr>, DTHFL <chr>, SITEID <chr>,
#> #   BRTHDTC <chr>, AGE <dbl>, AGEU <chr>, SEX <chr>, RACE <chr>, ETHNIC <chr>,
#> #   ARMCD <chr>, ARM <chr>, ACTARMCD <chr>, ACTARM <chr>, COUNTRY <chr>,
#> #   DMDTC <chr>, DMDY <dbl>, TRTSDTM <dttm>, TRTEDTM <dttm>, TRTSDT <date>,
#> #   TRTEDT <date>, TRTDURD <dbl>, SAFFL <chr>, TRT01P <chr>, TRT01A <chr>,
#> #   AGEGR1 <chr>, AGEGR1N <dbl>, RANDDT <date>, EOSDT <date>, EOSSTT <chr>, …
```

### Transactions for Atomic Updates

Transactions ensure that related table updates either all succeed or all
fail together, maintaining data consistency. This is critical when
adding derived variables that must stay synchronized across datasets.

Here’s an example of adding a new analysis flag to both ADSL and ADAE
atomically:

``` r
# Add ANALYSISFL to both ADSL and ADAE in a single atomic operation
# with_transaction() automatically handles rollback on error
with_transaction({
  # First, add the flag to ADSL
  get_ducklake_table("adsl") |>
    mutate(ANALYSISFL = if_else(SAFFL == "Y" & !is.na(TRTSDT), "Y", "N")) |>
    replace_table("adsl")  # Creates versioned snapshot
  
  # Then propagate to ADAE by joining
  adsl_flags <- get_ducklake_table("adsl") |>
    select(USUBJID, ANALYSISFL)
  
  get_ducklake_table("adae") |>
    select(-any_of("ANALYSISFL")) |>  # Remove if exists
    left_join(adsl_flags, by = "USUBJID") |>
    replace_table("adae")  # Creates versioned snapshot
  
  # Both updates succeed together
  cat("Both tables updated successfully\n")
}, author = "T Gerke", commit_message = "Add analysis flag")
#> Transaction started
#> Both tables updated successfully
#> Transaction committed
#> Snapshot metadata updated
```

This ensures ADSL and ADAE stay synchronized - either both get the new
`ANALYSISFL` column or neither does. The
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
function automatically handles rollback if any operation fails, making
it safer than manually managing transactions. Both updates are also
versioned for audit trails.

### Updating Records

Update existing records while maintaining version control and audit
trails:

``` r
# Update a specific record with versioning
with_transaction(
  get_ducklake_table("adae") |>
    mutate(
      AESEV = if_else(
        USUBJID == "01-701-1015" & AESEQ == 1,
        "SEVERE",
        AESEV
      )
    ) |>
    replace_table("adae"),  # Creates versioned snapshot
  author = "T Gerke",
  commit_message = "Correct AE severity"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Verify the update
get_ducklake_table("adae") |>
  filter(USUBJID == "01-701-1015", AESEQ == 1) |>
  select(USUBJID, AEDECOD, AESEV)
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#>   USUBJID     AEDECOD                   AESEV 
#>   <chr>       <chr>                     <chr> 
#> 1 01-701-1015 APPLICATION SITE ERYTHEMA SEVERE
```

## Querying and Analysis

The data lake enables efficient querying across all datasets:

``` r
# Example 1: Subject disposition summary
get_ducklake_table("adsl") |>
  count(EOSSTT, TRT01P) |>
  arrange(TRT01P, EOSSTT)
#> # Source:     SQL [?? x 3]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#> # Ordered by: TRT01P, EOSSTT
#>   EOSSTT    TRT01P                   n
#>   <chr>     <chr>                <dbl>
#> 1 COMPLETED Placebo                 86
#> 2 ONGOING   Screen Failure          52
#> 3 COMPLETED Xanomeline High Dose    84
#> 4 COMPLETED Xanomeline Low Dose     84

# Example 2: Treatment-emergent AE summary by severity
get_ducklake_table("adae") |>
  filter(TRTEMFL == "Y") |>
  count(TRT01A, AESEV) |>
  arrange(TRT01A, AESEV)
#> # Source:     SQL [?? x 3]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#> # Ordered by: TRT01A, AESEV
#>   TRT01A               AESEV        n
#>   <chr>                <chr>    <dbl>
#> 1 Placebo              MILD       209
#> 2 Placebo              MODERATE    65
#> 3 Placebo              SEVERE       7
#> 4 Xanomeline High Dose MILD       287
#> 5 Xanomeline High Dose MODERATE   115
#> 6 Xanomeline High Dose SEVERE      10
#> 7 Xanomeline Low Dose  MILD       232
#> 8 Xanomeline Low Dose  MODERATE   170
#> 9 Xanomeline Low Dose  SEVERE      25

# Example 3: PK concentration profile
get_ducklake_table("adpc") |>
  filter(PARAMCD == "XAN", EVID == 0) |>
  group_by(NFRLT) |>
  summarise(
    n = n(),
    mean_conc = mean(AVAL, na.rm = TRUE),
    sd_conc = sd(AVAL, na.rm = TRUE)
  ) |>
  arrange(NFRLT)
#> # Source:     SQL [?? x 4]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#> # Ordered by: NFRLT
#>    NFRLT     n mean_conc  sd_conc
#>    <dbl> <dbl>     <dbl>    <dbl>
#>  1  0      254   0        0      
#>  2  0.08   254   0.0682   0.0455 
#>  3  0.5    254   0.362    0.257  
#>  4  1      254   0.616    0.439  
#>  5  1.5    254   0.795    0.568  
#>  6  2      254   0.922    0.658  
#>  7  3      254  17.7     12.7    
#>  8  4      254   1.15     0.821  
#>  9  6      254   1.21     0.862  
#> 10  8      254   1.22     0.872  
#> 11  9      254  14.9     10.8    
#> 12 12      254   0.366    0.260  
#> 13 16      254   0.110    0.0767 
#> 14 18      254   9.39     6.92   
#> 15 24      254   0.0114   0.00520
#> 16 36      254   0.00500  0      
#> 17 37      254   0.165    0.426  
#> 18 48      254   0.00500  0

# Example 4: Cross-domain analysis: AEs by age group
get_ducklake_table("adae") |>
  filter(TRTEMFL == "Y") |>
  left_join(
    get_ducklake_table("adsl") |>
      select(USUBJID, AGEGR1, TRT01A),
    by = "USUBJID"
  ) |>
  count(AGEGR1, TRT01A.x) |>
  arrange(AGEGR1, TRT01A.x)
#> # Source:     SQL [?? x 3]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpKBTY0p/duckplyr/duckplyr1f211f250a0c.duckdb]
#> # Ordered by: AGEGR1, TRT01A.x
#>   AGEGR1 TRT01A.x                 n
#>   <chr>  <chr>                <dbl>
#> 1 18-64  Placebo                 57
#> 2 18-64  Xanomeline High Dose    86
#> 3 18-64  Xanomeline Low Dose     20
#> 4 >64    Placebo                224
#> 5 >64    Xanomeline High Dose   326
#> 6 >64    Xanomeline Low Dose    407
```

## Audit Trail and Compliance

For regulatory submissions, the complete audit trail is essential:

``` r
# Generate audit report for ADSL
audit_report <- list_table_snapshots("adsl")
audit_report
#>    snapshot_id       snapshot_time schema_version
#> 16          15 2026-02-06 22:46:37             15
#> 22          21 2026-02-06 22:46:40             21
#> 23          22 2026-02-06 22:46:40             22
#> 24          23 2026-02-06 22:46:40             23
#> 25          24 2026-02-06 22:46:40             24
#> 26          25 2026-02-06 22:46:41             25
#>                                                                                       changes
#> 16                                        tables_created, tables_inserted_into, main.adsl, 15
#> 22                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 15, 21
#> 23                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 21, 22
#> 24                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 22, 23
#> 25                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 23, 24
#> 26 tables_created, tables_dropped, tables_inserted_into, main.adsl, main.adae, 16, 24, 25, 26
#>     author              commit_message
#> 16 T Gerke         Create ADSL dataset
#> 22 T Gerke Add age categorization vars
#> 23 T Gerke      Test age categories v1
#> 24 T Gerke    Refine age categories v2
#> 25 T Gerke     Finalize age categories
#> 26 T Gerke           Add analysis flag
#>                                                                      commit_extra_info
#> 16 Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups
#> 22                                                                                <NA>
#> 23                                                                                <NA>
#> 24                                                                                <NA>
#> 25                                                                                <NA>
#> 26                                                                                <NA>

# Get table metadata from DuckLake system tables
adsl_table_meta <- get_metadata_table("ducklake_table") |>
  filter(table_name == "adsl") |>
  collect()
adsl_table_meta
#> # A tibble: 6 × 8
#>   table_id table_uuid     begin_snapshot end_snapshot schema_id table_name path 
#>      <dbl> <chr>                   <dbl>        <dbl>     <dbl> <chr>      <chr>
#> 1       15 019c3522-c796…             15           21         0 adsl       adsl/
#> 2       21 019c3522-d224…             21           22         0 adsl       adsl/
#> 3       22 019c3522-d2fe…             22           23         0 adsl       adsl/
#> 4       23 019c3522-d3b5…             23           24         0 adsl       adsl/
#> 5       24 019c3522-d46d…             24           25         0 adsl       adsl/
#> 6       26 019c3522-d613…             25           NA         0 adsl       adsl/
#> # ℹ 1 more variable: path_is_relative <lgl>

# Export audit information
audit_export <- audit_report |>
  mutate(
    table_name = "adsl",
    dataset_label = "Subject-Level Analysis Dataset"
  )
audit_export
#>    snapshot_id       snapshot_time schema_version
#> 16          15 2026-02-06 22:46:37             15
#> 22          21 2026-02-06 22:46:40             21
#> 23          22 2026-02-06 22:46:40             22
#> 24          23 2026-02-06 22:46:40             23
#> 25          24 2026-02-06 22:46:40             24
#> 26          25 2026-02-06 22:46:41             25
#>                                                                                       changes
#> 16                                        tables_created, tables_inserted_into, main.adsl, 15
#> 22                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 15, 21
#> 23                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 21, 22
#> 24                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 22, 23
#> 25                    tables_created, tables_dropped, tables_inserted_into, main.adsl, 23, 24
#> 26 tables_created, tables_dropped, tables_inserted_into, main.adsl, main.adae, 16, 24, 25, 26
#>     author              commit_message
#> 16 T Gerke         Create ADSL dataset
#> 22 T Gerke Add age categorization vars
#> 23 T Gerke      Test age categories v1
#> 24 T Gerke    Refine age categories v2
#> 25 T Gerke     Finalize age categories
#> 26 T Gerke           Add analysis flag
#>                                                                      commit_extra_info
#> 16 Derived from DM, SUPPDM, DS, EX; includes treatment dates, safety flags, age groups
#> 22                                                                                <NA>
#> 23                                                                                <NA>
#> 24                                                                                <NA>
#> 25                                                                                <NA>
#> 26                                                                                <NA>
#>    table_name                  dataset_label
#> 16       adsl Subject-Level Analysis Dataset
#> 22       adsl Subject-Level Analysis Dataset
#> 23       adsl Subject-Level Analysis Dataset
#> 24       adsl Subject-Level Analysis Dataset
#> 25       adsl Subject-Level Analysis Dataset
#> 26       adsl Subject-Level Analysis Dataset
```

## Cleanup

When you’re done, you can detach from the data lake:

``` r
detach_ducklake()
```

## Summary

This vignette demonstrated how **ducklake** provides a robust
infrastructure for clinical trial data management:

1.  **Setup**: Created a versioned data lake for clinical trial data
2.  **Medallion Architecture**: Implemented bronze (raw), silver
    (cleaned), and gold (analysis) layers
3.  **SDTM Loading**: Loaded multiple SDTM domains in both raw and
    cleaned versions with full version control
4.  **ADaM Derivation**: Built analysis datasets (ADSL, ADAE, ADPC) with
    complete data lineage from silver to gold
5.  **Regulatory Artifacts**: Stored define.xml, ARD, ARM, and
    specifications alongside datasets
6.  **Organization**: Maintained cohesive relationships between related
    datasets and documentation
7.  **Functionality**: Demonstrated versioning, time travel,
    transactions, and upserts
8.  **Analysis**: Showed efficient cross-domain queries
9.  **Compliance**: Generated audit trails for regulatory requirements
    with raw data preservation

By using ducklake for clinical trial data, you ensure:

- **Modern Architecture**: Relational database structure for inherently
  relational CDISC data
- **Layered Design**: Bronze/silver/gold layers separate raw, cleaned,
  and analysis-ready data
- **Reproducibility**: Analyses can be exactly recreated; raw data
  enables reprocessing
- **Traceability**: Complete lineage from raw source through cleaning to
  final analysis
- **Collaboration**: Multiple analysts working safely with shared data
  layers
- **Compliance**: Regulatory-ready audit trails with preserved source
  data
- **Efficiency**: Fast queries across related datasets without loading
  multiple flat files
- **Data Integrity**: Referential integrity checks across related tables
- **Reprocessability**: Ability to rerun cleaning or analysis logic
  without re-extracting from EDC

For more information on specific features:

- [`vignette("ducklake")`](https://tgerke.github.io/ducklake-r/articles/ducklake.md) -
  Getting started guide
- [`vignette("time-travel")`](https://tgerke.github.io/ducklake-r/articles/time-travel.md) -
  Time travel and version control
- [`vignette("transactions")`](https://tgerke.github.io/ducklake-r/articles/transactions.md) -
  Transaction management

## References

- [CDISC SDTM](https://www.cdisc.org/standards/foundational/sdtm)
- [CDISC ADaM](https://www.cdisc.org/standards/foundational/adam)
- [pharmaverse](https://pharmaverse.org/)
- [admiral](https://pharmaverse.github.io/admiral/)
- [pharmaversesdtm](https://pharmaverse.github.io/pharmaversesdtm/)
- [DuckLake Documentation](https://ducklake.select/docs/stable/)
