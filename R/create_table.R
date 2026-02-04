#' Create a DuckLake table
#'
#' @param table_name Name of the new table
#' @param data_source Raw data source. Can be:
#'   - A URL (http:// or https://)
#'   - A file path (e.g., "data.csv", "data.parquet")
#'   - An R data.frame or tibble
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' # From URL
#' create_table("my_table", "https://example.com/data.csv")
#' 
#' # From local file
#' create_table("my_table", "data.csv")
#' 
#' # From data.frame
#' create_table("my_table", mtcars)
#' }
create_table <- function(table_name, data_source) {
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
