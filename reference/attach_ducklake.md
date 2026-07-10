# Create or attach a ducklake

Wrapper for the ducklake
[ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting)
command. Creates a new DuckLake if the specified name does not exist, or
connects to an existing one. The lake can be detached with
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md).

## Usage

``` r
attach_ducklake(
  ducklake_name,
  lake_path,
  backend = c("duckdb", "postgres", "sqlite", "mysql"),
  catalog_connection_string = NULL,
  read_only = FALSE,
  override_data_path = FALSE,
  data_inlining_row_limit = NULL,
  encrypted = FALSE,
  snapshot_version = NULL,
  snapshot_time = NULL
)
```

## Arguments

- ducklake_name:

  Name for the ducklake, used as the database alias in DuckDB

- lake_path:

  Directory path where the lake lives. For `"duckdb"` this is where the
  catalog file and Parquet data are stored. For other backends this sets
  the Parquet data location (DuckLake's `DATA_PATH`), which may also be
  an object-storage URI such as `"s3://bucket/path"` – register
  credentials first with
  [`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md).
  (The `"duckdb"` backend needs a local `lake_path`, since its catalog
  is a database file.)

- backend:

  Catalog backend: `"duckdb"` (default), `"postgres"`, `"sqlite"`, or
  `"mysql"`.

- catalog_connection_string:

  Backend-specific connection string:

  `"duckdb"`

  :   Not required. Defaults to `{ducklake_name}.ducklake`.

  `"postgres"`

  :   libpq string, e.g. `"dbname=mydb host=localhost"`.

  `"sqlite"`

  :   Path to the SQLite file, e.g. `"metadata.sqlite"`.

  `"mysql"`

  :   MySQL connection string, e.g. `"db=mydb host=localhost"`.

- read_only:

  Attach in read-only mode (default `FALSE`).

- override_data_path:

  Override the stored DATA_PATH in the catalog (default `FALSE`). Needed
  when restoring a backup to a different location.

- data_inlining_row_limit:

  Optional integer. Sets the per-connection data inlining row limit.
  Inserts or deletes affecting fewer rows than this threshold are stored
  directly in the catalog instead of writing Parquet files. The default
  (when `NULL`) uses the DuckLake default of 10 rows. Set to `0` to
  disable inlining for this connection. This setting is not persisted;
  use
  [`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)
  for persistent overrides.

- encrypted:

  If `TRUE`, DuckLake encrypts the Parquet data files it writes.
  Encryption keys are stored in the catalog database, so anyone with
  access to the catalog can read the data – protect the catalog
  accordingly. Only applies when the lake is first created; an existing
  lake keeps the setting it was created with. The httpfs extension is
  loaded automatically: on some platforms (notably Windows) DuckDB's
  built-in crypto module is read-only and httpfs provides the writer.
  Default `FALSE`.

- snapshot_version:

  Optional snapshot id. Attaches the lake pinned to that snapshot:
  queries see the lake exactly as it was then, and writes are rejected.
  Mutually exclusive with `snapshot_time`.

- snapshot_time:

  Optional POSIXct or UTC timestamp string. Attaches the lake pinned to
  its state at that moment. Mutually exclusive with `snapshot_version`.

## Details

By default DuckDB is used as the catalog database. Alternative backends
(PostgreSQL, SQLite, MySQL) can be selected with the `backend`
parameter, which enables concurrent multi-client access. See
<https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database>.

For credential management with PostgreSQL or MySQL, consider DuckDB's
built-in secrets manager instead of embedding credentials in the
connection string:

    conn <- get_ducklake_connection()
    DBI::dbExecute(conn, "CREATE SECRET (
        TYPE postgres,
        HOST '127.0.0.1',
        PORT 5432,
        DATABASE ducklake_catalog,
        USER 'analyst',
        PASSWORD 'secret'
    )")

Then pass an empty or partial `catalog_connection_string`; DuckDB fills
in the rest from the secret. See
<https://duckdb.org/docs/stable/configuration/secrets_manager>.

**Windows limitation:** The `postgres` and `mysql` DuckDB extensions are
not available on Windows (MinGW toolchain). Only `duckdb` and `sqlite`
backends work there. Use Linux, macOS, or WSL for PostgreSQL/MySQL
backends. See <https://github.com/duckdb/duckdb/issues/7892>.

## See also

[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md)

Other connection management:
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# DuckDB catalog (default)
attach_ducklake("my_lake", lake_path = "~/data/lake")

# PostgreSQL catalog
attach_ducklake(
  "my_lake",
  backend = "postgres",
  catalog_connection_string = "dbname=ducklake_catalog host=localhost",
  lake_path = "/shared/lake/data/"
)

# SQLite catalog
attach_ducklake(
  "my_lake",
  backend = "sqlite",
  catalog_connection_string = "metadata.sqlite",
  lake_path = "data_files/"
)

# MySQL catalog
attach_ducklake(
  "my_lake",
  backend = "mysql",
  catalog_connection_string = "db=ducklake_catalog host=localhost",
  lake_path = "data_files/"
)

# Custom inlining threshold for streaming workload
attach_ducklake(
  "streaming_lake",
  lake_path = "~/data/streaming",
  data_inlining_row_limit = 100
)

# Encrypted Parquet files (keys live in the catalog)
attach_ducklake("secure_lake", lake_path = "~/data/secure", encrypted = TRUE)

# A frozen view of the lake as of snapshot 12, e.g. for reproducing a report
attach_ducklake("lake_v12", lake_path = "~/data/lake", snapshot_version = 12)
} # }
```
