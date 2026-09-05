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
  meta_encryption_key = NULL,
  snapshot_version = NULL,
  snapshot_time = NULL,
  automatic_migration = FALSE
)
```

## Arguments

- ducklake_name:

  Name for the ducklake, used as the database alias in DuckDB

- lake_path:

  Directory where the Parquet data files are stored (DuckLake's
  `DATA_PATH`). May be a local directory or an object-storage URI such
  as `"s3://bucket/path"` – register credentials first with
  [`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md).
  For `"duckdb"` the catalog file lives in this directory too by
  default; give `catalog_connection_string` to place it elsewhere, which
  is how a local catalog pairs with remote data.

- backend:

  Catalog backend: `"duckdb"` (default), `"postgres"`, `"sqlite"`, or
  `"mysql"`.

- catalog_connection_string:

  Backend-specific connection string:

  `"duckdb"`

  :   Optional path for the catalog database file. Defaults to
      `{lake_path}/{ducklake_name}.ducklake`. Set it to keep the catalog
      on local disk while `lake_path` points at object storage.

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

- meta_encryption_key:

  Optional key that encrypts the catalog database file itself, with
  AES-256-GCM (`"duckdb"` backend only; the `META_` prefix is how
  DuckLake forwards the option to the metadata catalog). The key takes
  effect when the catalog is first created – an existing unencrypted
  catalog cannot be encrypted after the fact – and the same key is
  required on every later attach, with no recovery if it is lost. Pairs
  naturally with `encrypted`, whose Parquet keys are stored in the
  catalog. Pass
  [`askpass::askpass()`](https://r-lib.r-universe.dev/askpass/reference/askpass.html)
  to be prompted rather than putting the key in code. The key is
  interpolated into the `ATTACH` statement text, so it can surface where
  statements are logged or profiled.

- snapshot_version:

  Optional snapshot id. Attaches the lake pinned to that snapshot:
  queries see the lake exactly as it was then, and writes are rejected.
  Mutually exclusive with `snapshot_time`.

- snapshot_time:

  Optional POSIXct or UTC timestamp string. Attaches the lake pinned to
  its state at that moment. Mutually exclusive with `snapshot_version`.

- automatic_migration:

  If `TRUE`, let DuckLake upgrade a catalog written in an earlier format
  version to the one the installed extension uses (the
  `AUTOMATIC_MIGRATION` option). Needed once for a lake created with
  DuckDB 1.5.1, whose extension wrote the 0.4 format, after upgrading to
  1.5.2 or later. The upgrade is permanent, so take a backup first.
  Default `FALSE`, in which case a mismatch is an error.

## Value

Invisibly, `NULL`. Called for its side effect of attaching the DuckLake
catalog to the package's DuckDB connection.

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
backends. See <https://github.com/duckdb/duckdb/issues/7892>. The `aws`
and `azure` extensions are missing on Windows too, so
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md)
with `provider = "credential_chain"` or `type = "azure"` does not work
there; explicit S3 keys do.

## See also

[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md)

Other connection management:
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
# DuckDB catalog (default)
lake_dir <- tempfile("my_lake_")
dir.create(lake_dir)
attach_ducklake("my_lake", lake_path = lake_dir)
detach_ducklake("my_lake")

# Custom inlining threshold for a streaming workload
stream_dir <- tempfile("streaming_lake_")
dir.create(stream_dir)
attach_ducklake(
  "streaming_lake",
  lake_path = stream_dir,
  data_inlining_row_limit = 100
)

detach_ducklake("streaming_lake", shutdown = TRUE)
unlink(c(lake_dir, stream_dir), recursive = TRUE)

# The remaining forms need a catalog server, or extensions that are
# downloaded on first use, so they are not run here.
if (FALSE) { # \dontrun{
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

# DuckDB catalog on local disk, Parquet data on S3
create_storage_secret("s3", provider = "credential_chain")
attach_ducklake(
  "trial_lake",
  lake_path = "s3://my-trial-lake/data",
  catalog_connection_string = "trial_lake.ducklake"
)

# Read-only attach of a .ducklake catalog straight from object storage
attach_ducklake(
  "trial_lake",
  lake_path = "s3://my-trial-lake/data",
  catalog_connection_string = "s3://my-trial-lake/trial_lake.ducklake",
  read_only = TRUE
)

# Encrypted Parquet files (keys live in the catalog); needs httpfs
attach_ducklake("secure_lake", lake_path = "path/to/lake", encrypted = TRUE)

# Encrypt the catalog database too -- it is where the Parquet keys live.
# askpass prompts for the key so it never sits in code.
attach_ducklake(
  "secure_lake",
  lake_path = "path/to/lake",
  encrypted = TRUE,
  meta_encryption_key = askpass::askpass("Catalog encryption key")
)

# A frozen view of the lake as of snapshot 12, e.g. to reproduce a report
attach_ducklake("lake_v12", lake_path = "path/to/lake", snapshot_version = 12)

# A lake created with DuckDB 1.5.1 (catalog format 0.4), opened after
# upgrading: migrate it once, then attach as usual
attach_ducklake("old_lake", lake_path = "path/to/lake", automatic_migration = TRUE)
} # }
```
