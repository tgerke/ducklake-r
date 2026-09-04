#' Set a DuckLake option
#'
#' Sets a DuckLake configuration option, either lake-wide or scoped to a
#' schema or table. Options are persisted in the metadata catalog, so they
#' survive detach/attach cycles and apply to every client of the lake.
#'
#' @param option Name of the option, e.g. `"parquet_compression"`,
#'   `"target_file_size"`, `"sort_on_insert"`, or
#'   `"data_inlining_row_limit"`. See
#'   \url{https://ducklake.select/docs/stable/duckdb/usage/configuration}
#'   for the full list.
#' @param value The value to set. Logicals are rendered as `true`/`false`,
#'   numbers as numeric literals, and everything else as a quoted string.
#' @param table_name Optional table name to scope the option to one table,
#'   optionally qualified as `"schema.table"`.
#' @param schema_name Optional schema name to scope the option to one schema
#'   (or, together with `table_name`, to qualify the table).
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @details
#' Table-scoped settings override schema-scoped ones, which override the
#' lake-wide default. Runs `CALL <lake>.set_option(...)`.
#'
#' The options DuckLake 1.0 persists, with their defaults:
#'
#' | Option | Default | What it controls |
#' |---|---|---|
#' | `auto_compact` | `true` | Whether maintenance calls made without a table argument include the table |
#' | `data_inlining_row_limit` | `10` | Rows below which an insert or delete is stored in the catalog instead of a file (see [set_inlining_row_limit()]) |
#' | `delete_older_than` | unset | How long a released file waits before [cleanup_old_files()] and checkpoints delete it |
#' | `expire_older_than` | unset | How old a snapshot must be before checkpoints expire it |
#' | `encrypted` | `false` | Encrypt the Parquet files written to the data path (set at creation; see `attach_ducklake(encrypted = )`) |
#' | `hive_file_pattern` | `true` | Write partitioned data in Hive-style directories |
#' | `parquet_compression` | `snappy` | Codec: `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, or `lz4_raw` |
#' | `parquet_compression_level` | `3` | Level for codecs that have one |
#' | `parquet_row_group_size` | `122880` | Rows per row group |
#' | `parquet_row_group_size_bytes` | unset | Bytes per row group, as an alternative to rows |
#' | `parquet_version` | `1` | Parquet format version, `1` or `2` |
#' | `per_thread_output` | `false` | One output file per thread during a parallel insert |
#' | `require_commit_message` | `false` | Refuse to commit a snapshot without a commit message |
#' | `rewrite_delete_threshold` | `0.95` | Deleted fraction of a file above which [rewrite_data_files()] rewrites it |
#' | `sort_on_insert` | `true` | Sort inserted rows by the table's sort keys (see [set_table_sorting()]) |
#' | `target_file_size` | `512MB` | Target data file size for inserts and compaction |
#' | `write_deletion_vectors` | `false` | Write Iceberg V3 deletion vectors instead of positional delete files |
#'
#' `created_by`, `data_path`, and `version` also appear in
#' [get_ducklake_options()] but describe the lake rather than configure it.
#' Retention settings (`expire_older_than`, `delete_older_than`) take
#' interval strings such as `"90 days"`.
#'
#' @returns Invisibly returns `NULL`.
#' @family options
#' @export
#'
#' @seealso [get_ducklake_options()], [set_inlining_row_limit()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("setopt_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("setopt_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Smaller files at some write cost, lake-wide
#' set_ducklake_option("parquet_compression", "zstd")
#'
#' # Skip one table during compaction
#' set_ducklake_option("auto_compact", FALSE, table_name = "cars")
#'
#' # Make every snapshot carry a commit message (constrains later writes)
#' set_ducklake_option("require_commit_message", TRUE)
#'
#' detach_ducklake("setopt_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
set_ducklake_option <- function(option,
                                value,
                                table_name = NULL,
                                schema_name = NULL,
                                ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)
  if (!is.null(table_name)) {
    ref <- resolve_table_ref(table_name, schema_name)
    table_name <- ref$table
    schema_name <- ref$schema
  }

  if (!is.character(option) || length(option) != 1 ||
      !grepl("^[a-z][a-z0-9_]*$", option)) {
    cli::cli_abort(
      "{.arg option} must be a single option name in snake_case."
    )
  }

  call_sql <- sprintf(
    "CALL %s.set_option(%s, %s%s);",
    ducklake_name,
    quote_sql(option),
    render_option_value(value),
    option_scope_args(table_name, schema_name)
  )
  db_execute(call_sql, conn = conn)

  scope <- if (!is.null(table_name)) {
    "table {.val {table_name}}"
  } else if (!is.null(schema_name)) {
    "schema {.val {schema_name}}"
  } else {
    "lake {.val {ducklake_name}}"
  }
  dl_inform(paste0(
    "Option {.val {option}} set to {.val {value}} for ", scope, "."
  ))

  invisible(NULL)
}

#' List the options set on a DuckLake
#'
#' Reads the configuration options recorded in the metadata catalog,
#' including their scope (global, schema, or table).
#'
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per option setting, including
#'   `option_name`, `value`, `scope` (`GLOBAL`, `SCHEMA`, or `TABLE`), and
#'   `scope_entry`. Options left at their defaults are not listed.
#' @family options
#' @export
#'
#' @seealso [set_ducklake_option()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("getopt_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("getopt_lake", lake_path = lake_dir)
#'
#' set_ducklake_option("parquet_compression", "zstd")
#' get_ducklake_options()
#'
#' detach_ducklake("getopt_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_ducklake_options <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  DBI::dbGetQuery(
    conn,
    sprintf("SELECT * FROM ducklake_options(%s);", quote_sql(ducklake_name))
  )
}

#' Render an R value as a set_option() SQL literal
#'
#' @noRd
render_option_value <- function(value) {
  if (length(value) != 1 || is.na(value)) {
    cli::cli_abort("{.arg value} must be a single non-missing value.")
  }
  if (is.logical(value)) {
    if (value) "true" else "false"
  } else if (is.numeric(value)) {
    format(value, scientific = FALSE)
  } else {
    quote_sql(as.character(value))
  }
}

#' Render optional schema/table scoping for set_option()
#'
#' @noRd
option_scope_args <- function(table_name, schema_name) {
  paste0(
    if (!is.null(schema_name)) {
      sprintf(", schema => %s", quote_sql(schema_name))
    } else {
      ""
    },
    if (!is.null(table_name)) {
      sprintf(", table_name => %s", quote_sql(table_name))
    } else {
      ""
    }
  )
}
