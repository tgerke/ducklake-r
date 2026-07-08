#' Update rows in a DuckLake table
#'
#' A wrapper around dplyr::rows_update() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with updates
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param unmatched How to handle unmatched rows (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_update()
#'
#' @details
#' ## When to use `rows_*()` vs [replace_table()]
#'
#' Use the `rows_*()` functions for **targeted, incremental changes**: appending
#' a batch of new records, correcting a handful of values, or removing specific
#' rows. Each call is a single SQL statement against the existing table -- no
#' data leaves the database, and with data inlining enabled (DuckLake's default)
#' small changes land in the catalog without creating tiny Parquet files.
#'
#' Use [replace_table()] for **structural or bulk changes**: adding or removing
#' columns, or transformations that touch most rows. It collects the transformed
#' data into R and rewrites the table, which is simpler for schema changes but
#' heavier for small edits.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' # Update rows - in_place = TRUE by default
#' rows_update(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = 1, value = "new"),
#'   by = "id"
#' )
#' }
rows_update <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, unmatched = "ignore", ...) {
  dplyr::rows_update(x = x, y = y, by = by, copy = copy, in_place = in_place, unmatched = unmatched, ...)
}

#' Prepare the `y` argument for a rows_* operation
#'
#' Local data frames are converted to an inline query on the same connection
#' as `x` via [dbplyr::copy_inline()]. Unlike dplyr's `copy = TRUE` path,
#' this creates no temporary table and starts no transaction of its own, so
#' rows_* calls work inside [with_transaction()] (DuckDB does not support
#' nested transactions).
#'
#' @param x Target lazy table
#' @param y Data frame or lazy table
#' @keywords internal
prep_rows_y <- function(x, y) {
  if (is.data.frame(y)) {
    dbplyr::copy_inline(dbplyr::remote_con(x), y)
  } else {
    y
  }
}

#' @exportS3Method dplyr::rows_update
rows_update.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     unmatched = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_update(
    x = x, y = y, by = by, ...,
    unmatched = unmatched, copy = copy, in_place = in_place
  )
}

#' Insert rows into a DuckLake table
#'
#' A wrapper around dplyr::rows_insert() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with new rows
#' @param by Column(s) to match on (for conflict detection)
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param conflict How to handle conflicts (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_insert()
#'
#' @details
#' ## When to use `rows_*()` vs [replace_table()]
#'
#' Use the `rows_*()` functions for **targeted, incremental changes**: appending
#' a batch of new records, correcting a handful of values, or removing specific
#' rows. Each call is a single SQL statement against the existing table -- no
#' data leaves the database, and with data inlining enabled (DuckLake's default)
#' small changes land in the catalog without creating tiny Parquet files.
#'
#' Use [replace_table()] for **structural or bulk changes**: adding or removing
#' columns, or transformations that touch most rows. It collects the transformed
#' data into R and rewrites the table, which is simpler for schema changes but
#' heavier for small edits.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' rows_insert(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = 99, value = "new row"),
#'   by = "id"
#' )
#' }
rows_insert <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, conflict = "ignore", ...) {
  dplyr::rows_insert(x = x, y = y, by = by, copy = copy, in_place = in_place, conflict = conflict, ...)
}

#' @exportS3Method dplyr::rows_insert
rows_insert.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     conflict = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_insert(
    x = x, y = y, by = by, ...,
    conflict = conflict, copy = copy, in_place = in_place
  )
}

#' Delete rows from a DuckLake table
#'
#' A wrapper around dplyr::rows_delete() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with rows to delete (matched by 'by' columns)
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param unmatched How to handle unmatched rows (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_delete()
#'
#' @details
#' ## When to use `rows_*()` vs [replace_table()]
#'
#' Use the `rows_*()` functions for **targeted, incremental changes**: appending
#' a batch of new records, correcting a handful of values, or removing specific
#' rows. Each call is a single SQL statement against the existing table -- no
#' data leaves the database, and with data inlining enabled (DuckLake's default)
#' small changes land in the catalog without creating tiny Parquet files.
#'
#' Use [replace_table()] for **structural or bulk changes**: adding or removing
#' columns, or transformations that touch most rows. It collects the transformed
#' data into R and rewrites the table, which is simpler for schema changes but
#' heavier for small edits.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' rows_delete(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = c(1, 2, 3)),
#'   by = "id"
#' )
#' }
rows_delete <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, unmatched = "ignore", ...) {
  dplyr::rows_delete(x = x, y = y, by = by, copy = copy, in_place = in_place, unmatched = unmatched, ...)
}

#' @exportS3Method dplyr::rows_delete
rows_delete.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     unmatched = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_delete(
    x = x, y = y, by = by, ...,
    unmatched = unmatched, copy = copy, in_place = in_place
  )
}
