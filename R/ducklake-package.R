#' @section Package options:
#' \describe{
#'   \item{`ducklake.author`}{The author recorded on the snapshots this
#'     session commits when a call does not name one: `commit_transaction()`,
#'     `with_transaction()`, `restore_table_version()`, and the creation
#'     snapshot `attach_ducklake()` labels. An `author` argument wins over
#'     the option. `set_snapshot_metadata()` does not read it, because
#'     labeling a snapshot after the fact is a deliberate edit, and a write
#'     made outside a transaction (`create_table()` on its own) records no
#'     author either way. Unset by default.}
#'   \item{`ducklake.verbose`}{When `FALSE`, the confirmations the package
#'     emits after each operation ("Committed snapshot 3: ...", "Added column
#'     ...") are suppressed, which keeps pipeline logs quiet. Warnings,
#'     errors, and notices about extension downloads are unaffected. The
#'     messages carry the condition class `ducklake_message`. Default
#'     `TRUE`.}
#' }
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom dplyr .data
## usethis namespace: end
NULL
