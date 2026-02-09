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

#' Upsert rows in a DuckLake table
#'
#' A wrapper around dplyr::rows_upsert() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications. Optionally adds
#' snapshot metadata after the operation completes.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with rows to upsert (update existing, insert new)
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param ... Additional arguments passed to dplyr::rows_upsert()
#'
#' @return The updated table
#' @export
#'
#' @details
#' This function performs an upsert operation: updates existing rows and inserts
#' new ones. Rows are matched using the columns specified in \code{by}.
#'
#' If \code{author}, \code{commit_message}, or \code{commit_extra_info} are provided,
#' they will be added to the snapshot metadata after the upsert completes.
#'
#' @seealso [upsert_table()] for pipeline-based upserts using dplyr transformations
#'
#' @examples
#' \dontrun{
#' # Basic upsert
#' rows_upsert(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = c(1, 99), value = c("updated", "new")),
#'   by = "id"
#' )
#' 
#' # Upsert with metadata
#' rows_upsert(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = c(1, 99), value = c("updated", "new")),
#'   by = "id",
#'   author = "Data Team",
#'   commit_message = "Update and add records"
#' )
#' }
rows_upsert <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, 
                        author = NULL, commit_message = NULL, commit_extra_info = NULL, ...) {
  # Perform the upsert operation (metadata params are NOT passed via ...)
  result <- dplyr::rows_upsert(x = x, y = y, by = by, copy = copy, in_place = in_place, ...)
  
  # Add metadata if any is provided
  if (!is.null(author) || !is.null(commit_message) || !is.null(commit_extra_info)) {
    conn <- get_ducklake_connection()
    # Get the current database name
    current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
    
    if (!is.null(current_db) && current_db != "") {
      set_snapshot_metadata(
        ducklake_name = current_db,
        author = author,
        commit_message = commit_message,
        commit_extra_info = commit_extra_info,
        conn = conn
      )
    }
  }
  
  invisible(result)
}

#' Patch rows in a DuckLake table
#'
#' A wrapper around dplyr::rows_patch() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with patches (only updates non-NA values)
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param unmatched How to handle unmatched rows (default "error")
#' @param ... Additional arguments passed to dplyr::rows_patch()
#'
#' @return The updated table
#' @export
#'
#' @examples
#' \dontrun{
#' # Patch (only update non-NA columns)
#' rows_patch(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = 1, col1 = "update", col2 = NA),
#'   by = "id"
#' )
#' }
rows_patch <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, unmatched = "ignore", ...) {
  dplyr::rows_patch(x = x, y = y, by = by, copy = copy, in_place = in_place, unmatched = unmatched, ...)
}
