#' Create a DuckLake table
#'
#' @param data_source Raw data source. Can be:
#'   - A URL (http:// or https://)
#'   - A file path (e.g., "data.csv", "data.parquet")
#'   - An R data.frame or tibble
#'   - A lazy table (tbl_duckdb_connection or tbl_lazy)
#' @param table_name Name of the new table
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' # From URL
#' create_table("https://example.com/data.csv", "my_table")
#' 
#' # From local file
#' create_table("data.csv", "my_table")
#' 
#' # From data.frame
#' create_table(mtcars, "my_table")
#' 
#' # From lazy table (pipe-friendly)
#' get_ducklake_table("source_table") %>% 
#'   filter(x > 5) %>%
#'   create_table("filtered_table")
#' }
create_table <- function(data_source, table_name) {
  # Handle lazy tables (tbl_duckdb_connection, tbl_lazy)
  if (inherits(data_source, "tbl_lazy")) {
    # Materialize the lazy table to a data.frame
    data_source <- dplyr::collect(data_source)
  }
  
  # Handle data.frame or tibble
  if (is.data.frame(data_source)) {
    # Register the data.frame as a temporary view in DuckDB
    temp_view_name <- paste0("__temp_view_", gsub("[^a-zA-Z0-9]", "_", table_name))
    duckdb::duckdb_register(get_ducklake_connection(), temp_view_name, data_source)
    
    # Create the table from the temporary view
    duckplyr::db_exec(sprintf("CREATE TABLE %s AS SELECT * FROM %s;", table_name, temp_view_name))
    
    # Unregister the temporary view
    duckdb::duckdb_unregister(get_ducklake_connection(), temp_view_name)
    
    return(invisible(NULL))
  }
  
  # If data_source is a URL, ensure httpfs extension is installed and loaded
  if (is.character(data_source) && grepl("^https?://", data_source)) {
    tryCatch({
      duckplyr::db_exec("LOAD httpfs;")
    }, error = function(e) {
      duckplyr::db_exec("INSTALL httpfs;")
      duckplyr::db_exec("LOAD httpfs;")
    })
  }
  
  # Handle file paths and URLs
  if (is.character(data_source)) {
    duckplyr::db_exec(sprintf("CREATE TABLE %s AS FROM '%s';", table_name, data_source))
  } else {
    stop("data_source must be a character string (file path or URL) or a data.frame")
  }
  
  invisible(NULL)
}
