#' Translate a dplyr pipeline into an in-place statement
#'
#' @param .data A dplyr query object (tbl_lazy) with accumulated operations
#' @param table_name Table name to update
#' @param .quiet Logical, whether to suppress debug output (default FALSE for backward compatibility)
#' @param .execute Logical, whether to execute the generated SQL (default
#'   TRUE). [show_ducklake_query()] passes FALSE to preview without running.
#'
#' @returns Invisibly returns the SQL statement string
#' @keywords internal
#' @noRd
#'
#' @details
#' The translation works on the structured query that dbplyr builds from the
#' pipeline ([dbplyr::sql_build()]), not on rendered SQL text. Pipelines
#' reading from `table_name` itself translate to:
#'
#' - `UPDATE` when `mutate()` reassigns existing columns (with `filter()`
#'   becoming the `WHERE` clause)
#' - `DELETE` of the rows *not* matching the filter when there is a
#'   `filter()` but no `mutate()`
#'
#' Pipelines reading from other tables translate to an `INSERT` that appends
#' their result into `table_name`, matching columns by name.
#'
#' **Limitations:**
#'
#' - **Cannot add or remove columns** - Only modifies values in existing columns
#' - **Simple queries only** - Pipelines that compile to nested subqueries
#'   over the target table (grouped filters, `mutate()` followed by
#'   `filter()`, window functions) are refused, as are `arrange()`,
#'   `head()`, `distinct()`, and aggregations
#'
#' Like all committed DuckLake changes, the UPDATE/DELETE/INSERT this
#' generates is recorded as a snapshot and can be time-traveled to.
#'
#' Use `replace_table()` if you need to:
#' - Add new derived columns
#' - Remove columns
#' - Apply transformations too complex to run in place
#'
#' Use `update_table()` when:
#' - Making targeted value corrections to existing columns
#' - Updating specific rows with filter(), without rewriting the table
#'
#' @examples
#' \dontrun{
#' # Correct a specific value (no versioning needed)
#' get_ducklake_table("adsl") |>
#'   mutate(SAFFL = if_else(USUBJID == "01-701-1015", "N", SAFFL)) |>
#'   update_table("adsl")
#'
#' # Update multiple columns
#' get_ducklake_table("adae") |>
#'   mutate(
#'     AESEV = if_else(AESEV == "MILD", "MODERATE", AESEV),
#'     AESER = if_else(AESEV == "SEVERE", "Y", AESER)
#'   ) |>
#'   update_table("adae")
#' }
update_table <- function(.data, table_name, .quiet = FALSE, .execute = TRUE) {

  if (!.quiet) {
    cli::cli_inform("Translating dplyr query into an in-place statement for {.val {table_name}}.")
  }

  result_sql <- tryCatch(
    build_in_place_sql(.data, table_name, .quiet = .quiet),
    error = function(e) {
      # Re-raise our own classified refusals untouched; wrap the unexpected
      if (inherits(e, "ducklake_translation_error")) {
        stop(e)
      }
      msg <- conditionMessage(e)
      cli::cli_abort("Failed to generate DuckLake SQL: {msg}")
    }
  )

  if (!.quiet) cli::cli_inform("Generated SQL: {.code {result_sql}}")

  if (.execute) {
    db_execute(result_sql)
  }

  # Return invisibly for potential chaining
  invisible(result_sql)
}

#' Build an UPDATE/DELETE/INSERT statement from a lazy tbl
#'
#' Classifies the pipeline from dbplyr's structured query object rather than
#' from rendered SQL text, so string literals containing SQL keywords,
#' commas inside function calls, and quoted identifiers cannot confuse it.
#'
#' @param .data A tbl_lazy.
#' @param table_name The target table.
#' @param .quiet Suppress progress messages.
#' @returns The SQL statement string.
#' @noRd
build_in_place_sql <- function(.data, table_name, .quiet = FALSE) {
  con <- dbplyr::remote_con(.data)
  qry <- dbplyr::sql_build(.data)
  target <- bare_table_name(table_name)
  quoted_table <- quote_ident(table_name, con)

  refuse_self_insert <- function() {
    cli::cli_abort(
      c(
        "The dplyr query has no filter or mutate to translate, and inserting a table's own rows back into it would duplicate them.",
        "i" = "Use {.fn rows_insert} to append new records, or {.fn replace_table} to rewrite the table."
      ),
      class = "ducklake_translation_error",
      call = NULL
    )
  }

  # A plain read of the target table itself: nothing to translate, and an
  # INSERT would duplicate every row
  if (inherits(qry, "base_query") && bare_table_name(qry$from) == target) {
    refuse_self_insert()
  }

  # A single SELECT directly over the target table: UPDATE or DELETE
  if (inherits(qry, "select_query") &&
      inherits(qry$from, "base_query") &&
      bare_table_name(qry$from$from) == target) {

    select <- qry$select
    nms <- names(select)
    if (is.null(nms)) nms <- rep("", length(select))
    values <- as.character(select)

    # A select entry is an assignment when its SQL is not just the column
    # itself (quoted or not); unnamed entries are the `table.*` passthrough
    quoted_nms <- ifelse(
      nzchar(nms),
      vapply(nms, function(nm) as.character(DBI::dbQuoteIdentifier(con, nm)), character(1)),
      ""
    )
    is_star <- !nzchar(nms)
    is_assignment <- nzchar(nms) & values != nms & values != quoted_nms

    where <- as.character(qry$where)

    if (!any(is_assignment) && length(where) == 0) {
      refuse_self_insert()
    }

    # Clauses that have no equivalent in an in-place statement
    extras <- c(
      if (isTRUE(qry$distinct)) "DISTINCT",
      if (!is.null(qry$limit)) "LIMIT",
      if (length(qry$order_by) > 0) "ORDER BY",
      if (length(qry$group_by) > 0) "GROUP BY",
      if (length(qry$having) > 0) "HAVING",
      if (length(qry$window) > 0) "WINDOW"
    )
    if (length(extras) > 0) {
      cli::cli_abort(
        c(
          "The dplyr query is too complex for an in-place update: {.val {extras}} cannot be translated.",
          "i" = "Use {.fn replace_table} for complex transformations, or {.fn ducklake_exec} with explicit SQL."
        ),
        class = "ducklake_translation_error",
        call = NULL
      )
    }

    where_sql <- if (length(where) > 1) {
      paste0("(", where, ")", collapse = " AND ")
    } else {
      where
    }

    if (any(is_assignment)) {
      # mutate() that only adds columns keeps the `table.*` passthrough;
      # an UPDATE cannot add columns
      if (any(is_star)) {
        new_cols <- nms[is_assignment]
        cli::cli_abort(
          c(
            "{.fn mutate} adds new column{?s} {.val {new_cols}}, but an in-place update cannot add columns.",
            "i" = "Use {.fn replace_table} to rewrite the table with new columns."
          ),
          class = "ducklake_translation_error",
          call = NULL
        )
      }
      if (!.quiet) cli::cli_inform("Operation type: {.val update}")
      assignments <- paste0(
        quoted_nms[is_assignment], " = ", values[is_assignment],
        collapse = ", "
      )
      result_sql <- sprintf("UPDATE %s SET %s", quoted_table, assignments)
      if (length(where) > 0) {
        result_sql <- paste(result_sql, "WHERE", where_sql)
      }
      return(result_sql)
    }

    # filter() only: keep the matching rows, delete the rest
    if (!.quiet) cli::cli_inform("Operation type: {.val delete}")
    return(sprintf("DELETE FROM %s WHERE NOT (%s)", quoted_table, where_sql))
  }

  # Everything else: the pipeline reads other tables (or the target through
  # a shape we can't take apart). Reading other tables appends into the
  # target; anything involving the target itself is refused.
  sources <- query_base_tables(qry)
  if (anyNA(sources)) {
    cli::cli_abort(
      c(
        "The dplyr query is too complex for an in-place update: the tables it reads could not be determined.",
        "i" = "Use {.fn replace_table} for complex transformations, or {.fn ducklake_exec} with explicit SQL."
      ),
      class = "ducklake_translation_error",
      call = NULL
    )
  }
  if (target %in% vapply(sources, bare_table_name, character(1))) {
    cli::cli_abort(
      c(
        "The dplyr query is too complex for an in-place update: {.val {target}} is read through a subquery, join, or set operation.",
        "i" = "Use {.fn replace_table} for complex transformations, or {.fn ducklake_exec} with explicit SQL."
      ),
      class = "ducklake_translation_error",
      call = NULL
    )
  }

  if (!.quiet) cli::cli_inform("Operation type: {.val insert}")
  insert_cols <- vapply(
    colnames(.data),
    function(nm) as.character(DBI::dbQuoteIdentifier(con, nm)),
    character(1)
  )
  sprintf(
    "INSERT INTO %s (%s) %s",
    quoted_table,
    paste(insert_cols, collapse = ", "),
    as.character(dbplyr::remote_query(.data))
  )
}

#' Collect the base tables a built query reads from
#'
#' Walks dbplyr query objects ([dbplyr::sql_build()] output) and returns the
#' table paths of every base table referenced. Returns `NA` when the query
#' contains a shape it cannot see into, so callers can refuse rather than
#' guess.
#'
#' @param qry A dbplyr query object.
#' @returns A character vector of table paths, possibly containing `NA`.
#' @noRd
query_base_tables <- function(qry) {
  if (inherits(qry, "base_query")) {
    return(as.character(qry$from))
  }
  if (inherits(qry, "select_query")) {
    return(query_base_tables(qry$from))
  }
  if (inherits(qry, "query")) {
    # Joins, set operations, and future query types: walk any nested query
    # objects in their fields. (Join queries also carry a `table_names`
    # field, but it holds aliases like `t_LHS`, not the underlying tables.)
    found <- unlist(
      lapply(unclass(qry), find_nested_query_tables),
      use.names = FALSE
    )
    if (length(found) > 0) {
      return(found)
    }
  }
  NA_character_
}

#' @noRd
find_nested_query_tables <- function(x) {
  if (inherits(x, "query")) {
    return(query_base_tables(x))
  }
  if (is.list(x)) {
    return(unlist(lapply(x, find_nested_query_tables), use.names = FALSE))
  }
  character(0)
}

#' Reduce a (possibly qualified, possibly quoted) table reference to its
#' bare, lowercased table name for comparison
#'
#' DuckDB identifiers are case-insensitive, and dbplyr table paths may be
#' schema-qualified (`lake.main.cars`) or quoted.
#'
#' @param x A table name, ident, or dbplyr table path.
#' @returns A single lowercased string.
#' @noRd
bare_table_name <- function(x) {
  x <- gsub('"', "", as.character(x)[[1]])
  parts <- strsplit(x, ".", fixed = TRUE)[[1]]
  if (length(parts) == 0) {
    return("")
  }
  tolower(parts[[length(parts)]])
}
