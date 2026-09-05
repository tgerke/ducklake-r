# Build the ATTACH SQL for a DuckLake

Build the ATTACH SQL for a DuckLake

## Usage

``` r
build_attach_sql(
  ducklake_name,
  lake_path,
  backend,
  catalog_connection_string,
  read_only,
  override_data_path = FALSE,
  data_inlining_row_limit = NULL,
  encrypted = FALSE,
  meta_encryption_key = NULL,
  snapshot_version = NULL,
  snapshot_time = NULL,
  automatic_migration = FALSE,
  create = TRUE,
  metadata_schema = NULL
)
```

## Arguments

- ducklake_name:

  Name for the ducklake alias

- lake_path:

  Path for data files

- backend:

  Catalog backend type

- catalog_connection_string:

  Backend-specific connection string; for the duckdb backend, an
  optional path for the catalog file

- read_only:

  Whether to attach in read-only mode

- override_data_path:

  Whether to add OVERRIDE_DATA_PATH TRUE

- data_inlining_row_limit:

  Optional integer for DATA_INLINING_ROW_LIMIT

- encrypted:

  Whether to add ENCRYPTED TRUE

- meta_encryption_key:

  Optional key for META_ENCRYPTION_KEY

- snapshot_version:

  Optional snapshot id for SNAPSHOT_VERSION

- snapshot_time:

  Optional timestamp for SNAPSHOT_TIME

- automatic_migration:

  Whether to add AUTOMATIC_MIGRATION

- create:

  Whether to allow creating the lake (CREATE_IF_NOT_EXISTS)

- metadata_schema:

  Optional schema for METADATA_SCHEMA

## Value

A SQL ATTACH statement string
