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
  data_inlining_row_limit = NULL
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

  Backend-specific connection string

- read_only:

  Whether to attach in read-only mode

- override_data_path:

  Whether to add OVERRIDE_DATA_PATH TRUE

- data_inlining_row_limit:

  Optional integer for DATA_INLINING_ROW_LIMIT

## Value

A SQL ATTACH statement string
