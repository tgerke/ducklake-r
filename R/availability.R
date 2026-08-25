#' Is the ducklake DuckDB extension usable?
#'
#' Reports whether the `ducklake` DuckDB extension is already installed and
#' can be loaded. The check opens a throwaway in-memory DuckDB connection with
#' automatic extension installation turned off, so it never downloads anything
#' and never writes to the extension cache in your home directory. It also
#' leaves the package's own connection untouched.
#'
#' Use it to guard code that should degrade gracefully when the extension is
#' absent: examples, vignettes, tests, and conditional branches in scripts.
#' The package's own examples are gated on it. To install the extension, call
#' [install_ducklake()].
#'
#' The result is cached for the rest of the session, since spinning up DuckDB
#' to re-answer the same question is wasteful when dozens of examples ask it.
#' [install_ducklake()] clears the cache, so a `FALSE` answer becomes `TRUE`
#' as soon as you install.
#'
#' @returns A single logical: `TRUE` when the extension loads, `FALSE`
#'   otherwise (including when a connection cannot be opened).
#' @family connection management
#' @export
#'
#' @examples
#' if (ducklake_extension_available()) {
#'   message("ducklake extension is ready to use")
#' } else {
#'   message("run install_ducklake() first")
#' }
ducklake_extension_available <- function() {
  cached <- .ducklake_env$ext_available
  if (!is.null(cached)) {
    return(cached)
  }

  ok <- tryCatch(
    {
      con <- DBI::dbConnect(duckdb::duckdb())
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
      # Without this, a failed LOAD silently downloads the extension into
      # ~/.duckdb/extensions/ -- exactly what this probe must not do.
      DBI::dbExecute(con, "SET autoinstall_known_extensions=false;")
      DBI::dbExecute(con, "LOAD ducklake;")
      TRUE
    },
    error = function(e) FALSE
  )

  .ducklake_env$ext_available <- ok
  ok
}

#' Forget a cached extension-availability answer
#'
#' @returns Invisibly, `NULL`.
#' @noRd
reset_extension_available_cache <- function() {
  .ducklake_env$ext_available <- NULL
  invisible(NULL)
}
