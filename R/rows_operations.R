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
#' @param unmatched How to handle unmatched rows (default "error")
#' @param ... Additional arguments passed to dplyr::rows_update()
#'
#' @return The updated table
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
#' @param conflict How to handle conflicts (default "error")
#' @param ... Additional arguments passed to dplyr::rows_insert()
#'
#' @return The updated table
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
#' @param unmatched How to handle unmatched rows (default "error")
#' @param ... Additional arguments passed to dplyr::rows_delete()
#'
#' @return The updated table
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
