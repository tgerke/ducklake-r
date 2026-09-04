#' Get a DuckLake table
#'
#' Returns a lazy reference to a table in the attached DuckLake. Like
#' [dplyr::tbl()], nothing is read until you `collect()`: build up your
#' `filter()`/`mutate()`/`summarise()` pipeline first and DuckDB executes it
#' as a single query, only pulling the rows you asked for into R.
#'
#' @param tbl_name Character string, name of the table to retrieve. A
#'   table outside the `main` schema is named `"schema.table"`.
#'
#' @returns A lazy table (class `tbl_ducklake`) that works with dplyr verbs.
#'   The table name is stored in the `ducklake_table_name` attribute.
#' @family table operations
#' @export
#'
#' @seealso [create_table()] to create tables, [get_ducklake_table_asof()]
#'   and [get_ducklake_table_version()] for time-travel reads.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("cars_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("cars_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Query lazily with dplyr, then collect
#' get_ducklake_table("cars") |>
#'   dplyr::filter(cyl > 4) |>
#'   dplyr::summarise(avg_mpg = mean(mpg), .by = cyl) |>
#'   dplyr::collect()
#'
#' detach_ducklake("cars_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_ducklake_table <- function(tbl_name) {
  conn <- get_ducklake_connection()
  if (grepl(".", tbl_name, fixed = TRUE)) {
    # The duckdb driver's tbl() method turns a dotted name it cannot find
    # as a literal table name into raw SQL, which reads fine but has no
    # table identity for rows_*() to write to. Hand dbplyr the quoted path.
    tbl <- dbplyr::tbl_sql(
      "duckdb_connection",
      src = dbplyr::src_dbi(conn),
      from = I(quote_ident(tbl_name, conn))
    )
  } else {
    tbl <- dplyr::tbl(conn, tbl_name)
  }
  as_ducklake_tbl(tbl, tbl_name)
}

#' Mark a lazy table as a DuckLake table
#'
#' Records the table name (for [ducklake_exec()] and for restoring labels
#' on [dplyr::collect()]) and adds the `tbl_ducklake` class so the
#' package's `rows_*()` methods dispatch regardless of package load order.
#'
#' @param tbl A lazy table.
#' @param table_name The DuckLake table it reads.
#' @returns `tbl` with the class and attribute set.
#' @noRd
as_ducklake_tbl <- function(tbl, table_name) {
  attr(tbl, "ducklake_table_name") <- table_name
  class(tbl) <- c("tbl_ducklake", setdiff(class(tbl), "tbl_ducklake"))
  tbl
}

#' Get a DuckLake metadata table
#'
#' DuckLake keeps all of its bookkeeping -- snapshots, table schemas, data
#' file locations, and more -- in ordinary tables inside the catalog
#' database. This function gives you a lazy reference to any of them, which
#' is handy for auditing and for understanding how your lake evolves.
#'
#' Commonly useful tables include `ducklake_snapshot` (one row per
#' snapshot), `ducklake_table` (registered tables), and `ducklake_data_file`
#' (the Parquet files backing each table). The full list is in the
#' [DuckLake specification](https://ducklake.select/docs/stable/specification/introduction).
#'
#' @param tbl_name Character string, name of the metadata table to retrieve
#'   (e.g., `"ducklake_snapshot"`).
#' @param ducklake_name Character string, name of the ducklake database
#'   (optional, defaults to the currently active ducklake).
#'
#' @returns A lazy table that works with dplyr verbs.
#' @family table operations
#' @export
#'
#' @seealso [list_table_snapshots()] for a friendlier view of snapshot history.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("meta_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("meta_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Every snapshot ever taken
#' get_metadata_table("ducklake_snapshot") |> dplyr::collect()
#'
#' # Which Parquet files back the lake?
#' get_metadata_table("ducklake_data_file") |>
#'   dplyr::select(data_file_id, path) |>
#'   dplyr::collect()
#'
#' detach_ducklake("meta_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_metadata_table <- function(tbl_name, ducklake_name = NULL) {
  # If ducklake_name not provided, try to infer from current database
  if (is.null(ducklake_name)) {
    conn <- get_ducklake_connection()
    tryCatch({
      current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
      if (!is.null(current_db) && current_db != "") {
        ducklake_name <- current_db
      }
    }, error = function(e) {
      cli::cli_abort("Could not determine {.arg ducklake_name}. Please provide it explicitly.")
    })
  }
  
  # Metadata tables are in the __ducklake_metadata_[ducklake_name] database.
  # DuckDB and SQLite use a .main. schema qualifier; PostgreSQL and MySQL do not.
  backend <- get_ducklake_backend(ducklake_name)
  if (backend %in% c("postgres", "mysql")) {
    metadata_tbl_name <- paste0("__ducklake_metadata_", ducklake_name, ".", tbl_name)
  } else {
    metadata_tbl_name <- paste0("__ducklake_metadata_", ducklake_name, ".main.", tbl_name)
  }
  return(get_ducklake_table(metadata_tbl_name))
}
