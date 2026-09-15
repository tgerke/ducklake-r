# Tests for the interactive change viewer: view_table_changes() and the
# payload it draws, which build_changes_payload() computes in R.

# Four rows, then two updates, one delete, and two inserts: 11 feed rows,
# four snapshots with an author and a message
build_audit_table <- function() {
  suppressMessages({
    create_table(
      data.frame(id = 1:4, amount = c(10, 20, 30, 40), label = c("a", "b", "c", "d")),
      "audit"
    )
    with_transaction(
      rows_update(get_ducklake_table("audit"), data.frame(id = 2L, amount = 99), by = "id"),
      author = "Auditor", commit_message = "Fix amount for id 2"
    )
    with_transaction(
      rows_update(get_ducklake_table("audit"), data.frame(id = 3L, label = "cc"), by = "id"),
      author = "Auditor", commit_message = "Fix label for id 3"
    )
    with_transaction(
      rows_delete(get_ducklake_table("audit"), data.frame(id = 4L), by = "id"),
      author = "Cleaner", commit_message = "Drop id 4"
    )
    with_transaction(
      rows_insert(
        get_ducklake_table("audit"),
        data.frame(id = 5:6, amount = c(50, 60), label = c("e", "f")),
        by = "id"
      ),
      author = "Loader", commit_message = "Add ids 5 and 6"
    )
  })
}

# One ALTER of each kind, returning the snapshot id each one made
build_evo_table <- function() {
  at <- function() max(list_table_snapshots("evo")$snapshot_id)
  suppressMessages({
    create_table(
      data.frame(id = 1:3, v = c(1.5, 2.5, 3.5), scratch = c("a", "b", "c")),
      "evo"
    )
    ids <- list(created = at())
    add_table_column("evo", "note", "VARCHAR")
    ids$added <- at()
    drop_table_column("evo", "scratch")
    ids$dropped <- at()
    rename_table_column("evo", "v", "value")
    ids$renamed <- at()
    set_column_type("evo", "id", "BIGINT")
    ids$widened <- at()
    set_column_not_null("evo", "value")
    ids$required <- at()
  })
  ids
}

test_that("build_changes_payload pairs update images and counts changed cells", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()

  p <- build_changes_payload(get_table_changes("audit"))

  expect_equal(
    p$counts[c("inserted", "updated", "deleted", "partial", "cells")],
    list(inserted = 6L, updated = 2L, deleted = 1L, partial = 0L, cells = 2L)
  )
  expect_equal(p$cells$column, c("amount", "label"))
  expect_equal(p$cells$old, c("20", "c"))
  expect_equal(p$cells$new, c("99", "cc"))
  expect_equal(p$columns$name, c("id", "amount", "label"))
  expect_equal(p$columns$type, c("integer", "number", "text"))
  expect_equal(p$columns$changed_cells, c(0L, 1L, 1L))

  # An update points at both images; an insert has no old side, a delete no
  # new side; every item names its snapshot and rowid
  updates <- p$items[p$items$kind == "update", ]
  expect_equal(updates$n_changed, c(1L, 1L))
  expect_false(anyNA(updates$old))
  expect_false(anyNA(updates$new))
  expect_true(all(is.na(p$items$old[p$items$kind == "insert"])))
  expect_true(all(is.na(p$items$new[p$items$kind == "delete"])))
  expect_false(anyNA(p$items$snapshot_id))
  expect_false(anyNA(p$items$rowid))
  expect_equal(p$items$snapshot_id, sort(p$items$snapshot_id))

  # The cells point at items, and the display strings are per feed row
  expect_equal(p$items$kind[p$cells$item], c("update", "update"))
  expect_equal(as.character(p$data$amount[p$items$old[p$cells$item[1]]]), "20")
  expect_equal(as.character(p$data$amount[p$items$new[p$cells$item[1]]]), "99")

  snaps <- list_table_snapshots("audit")
  expect_equal(p$title, "audit")
  expect_equal(p$table_name, "main.audit")
  expect_equal(p$ducklake_name, lake$ducklake_name)
  expect_equal(p$range$bound_type, "snapshot")
  expect_equal(c(p$range$start_id, p$range$end_id), range(snaps$snapshot_id))
  expect_false(p$truncated)
  expect_equal(p$counts$rows_shown, 11L)
  expect_equal(p$counts$rows_total, 11)
})

test_that("snapshots in range carry metadata and per-snapshot counts", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()

  p <- build_changes_payload(get_table_changes("audit"))
  snaps <- list_table_snapshots("audit")

  expect_equal(p$snapshots$snapshot_id, snaps$snapshot_id)
  expect_false(anyNA(p$snapshots$time))
  fix <- p$snapshots[which(p$snapshots$commit_message == "Fix amount for id 2"), ]
  expect_equal(nrow(fix), 1)
  expect_equal(fix$author, "Auditor")
  expect_equal(c(fix$inserted, fix$updated, fix$deleted), c(0L, 1L, 0L))
  expect_equal(sum(p$snapshots$inserted), 6L)
  expect_equal(sum(p$snapshots$updated), 2L)
  expect_equal(sum(p$snapshots$deleted), 1L)
  expect_equal(p$snapshots$schema_events, rep(0L, nrow(snaps)))
})

test_that("a dplyr filter that keeps one image of an update yields a one-sided item", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()

  p <- build_changes_payload(
    get_table_changes("audit") |> dplyr::filter(amount == 99)
  )

  expect_equal(nrow(p$items), 1)
  expect_equal(p$items$kind, "update")
  expect_true(p$items$partial)
  expect_true(is.na(p$items$old))
  expect_false(is.na(p$items$new))
  expect_true(is.na(p$items$n_changed))
  expect_equal(nrow(p$cells), 0)
  expect_equal(p$counts$partial, 1L)
  # The other snapshots in range keep their entries, with no rows
  expect_equal(sum(p$snapshots$updated), 1L)
  expect_equal(sum(p$snapshots$inserted), 0L)
})

test_that("the row cap is deterministic and never splits an update pair", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()
  feed <- get_table_changes("audit")

  # Row 5 is one image of the first update, so the cut lands before it
  p5 <- build_changes_payload(feed, max_rows = 5)
  expect_true(p5$truncated)
  expect_equal(p5$counts$rows_total, 11)
  expect_equal(p5$counts$rows_shown, 4L)
  expect_equal(p5$counts$partial, 0L)
  expect_equal(p5$items$kind, rep("insert", 4))

  p6 <- build_changes_payload(feed, max_rows = 6)
  expect_true(p6$truncated)
  expect_equal(p6$counts$rows_shown, 6L)
  expect_equal(p6$counts$updated, 1L)
  expect_equal(p6$counts$partial, 0L)

  expect_false(build_changes_payload(feed, max_rows = 11)$truncated)
  expect_false(build_changes_payload(feed, max_rows = Inf)$truncated)
})

test_that("a collected data frame works, with metadata only when the table is named", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()
  df <- dplyr::collect(get_table_changes("audit"))
  expect_null(attr(df, "ducklake_changes"))

  bare <- build_changes_payload(df)
  expect_equal(bare$counts$updated, 2L)
  expect_equal(bare$counts$cells, 2L)
  expect_false(bare$schema$available)
  expect_true(all(is.na(bare$snapshots$time)))
  expect_equal(bare$title, "Table changes")
  expect_true(is.na(bare$range$bound_type))

  named <- build_changes_payload(df, table_name = "audit")
  expect_true(named$schema$available)
  expect_false(anyNA(named$snapshots$time))
  expect_equal(named$snapshots$snapshot_id, sort(unique(df$snapshot_id)))
  expect_equal(named$title, "audit")
})

test_that("schema history classifies add, drop, rename, type change, and not null", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  ids <- build_evo_table()

  p <- build_changes_payload(get_table_changes("evo"))
  expect_true(p$schema$available)
  ev <- p$schema$events
  event <- function(snapshot) ev[ev$snapshot_id == snapshot, ]

  expect_equal(event(ids$created)$kind, rep("created", 3))
  expect_equal(event(ids$created)$column_name, c("id", "v", "scratch"))
  expect_equal(event(ids$added)$kind, "added")
  expect_equal(event(ids$added)$column_name, "note")
  expect_equal(event(ids$added)$detail, "varchar")
  expect_equal(event(ids$dropped)$kind, "dropped")
  expect_equal(event(ids$dropped)$column_name, "scratch")
  expect_equal(event(ids$renamed)$kind, "renamed")
  expect_equal(event(ids$renamed)$old_name, "v")
  expect_equal(event(ids$renamed)$new_name, "value")
  expect_equal(event(ids$renamed)$detail, "v -> value")
  expect_equal(event(ids$widened)$kind, "type_changed")
  expect_equal(event(ids$widened)$column_name, "id")
  expect_equal(event(ids$widened)$old_type, "int32")
  expect_equal(event(ids$widened)$new_type, "int64")
  expect_equal(event(ids$required)$kind, "altered")
  expect_equal(event(ids$required)$column_name, "value")
  expect_equal(event(ids$required)$detail, "NOT NULL")

  # The roster: current columns in order, then what was dropped in range
  expect_equal(p$schema$columns$name, c("id", "value", "note", "scratch"))
  expect_equal(p$schema$columns$type, c("int64", "float64", "varchar", "varchar"))
  expect_equal(p$schema$columns$status, c("type_changed", "altered", "added", "dropped"))

  # Counts leave the creation out; schema-only snapshots are listed
  expect_equal(p$counts$schema_events, 5L)
  expect_equal(p$snapshots$schema_events[p$snapshots$snapshot_id == ids$created], 0L)
  expect_equal(p$snapshots$schema_events[p$snapshots$snapshot_id == ids$renamed], 1L)
  expect_true(ids$required %in% p$snapshots$snapshot_id)

  # Narrowing the range narrows the events
  late <- build_changes_payload(get_table_changes("evo", ids$renamed, ids$required))
  expect_equal(late$schema$events$kind, c("renamed", "type_changed", "altered"))
  expect_equal(late$schema$columns$name, c("id", "value", "note"))
  expect_equal(late$schema$columns$status, c("type_changed", "altered", "unchanged"))
})

test_that("ducklake_column keeps the column_id across a rename and a type change", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  ids <- build_evo_table()

  tables <- get_metadata_table("ducklake_table") |> dplyr::collect()
  cols <- get_metadata_table("ducklake_column") |> dplyr::collect()
  cols <- cols[cols$table_id %in% tables$table_id[tables$table_name == "evo"], ]

  # A rename closes the old version and opens a new one at the same
  # snapshot: end_snapshot is exclusive. Only the changed column gets a
  # version; ADD COLUMN leaves the others alone.
  v <- cols[cols$column_name %in% c("v", "value"), ]
  v <- v[order(v$begin_snapshot), ]
  expect_equal(nrow(v), 3)
  expect_length(unique(v$column_id), 1)
  expect_equal(v$column_name, c("v", "value", "value"))
  expect_equal(v$end_snapshot, c(ids$renamed, ids$required, NA))
  expect_equal(v$begin_snapshot, c(ids$created, ids$renamed, ids$required))
  expect_equal(v$nulls_allowed, c(TRUE, TRUE, FALSE))

  id <- cols[cols$column_name == "id", ]
  id <- id[order(id$begin_snapshot), ]
  expect_length(unique(id$column_id), 1)
  expect_equal(id$column_type, c("int32", "int64"))
  expect_equal(id$end_snapshot, c(ids$widened, NA))

  scratch <- cols[cols$column_name == "scratch", ]
  expect_equal(nrow(scratch), 1)
  expect_equal(scratch$end_snapshot, ids$dropped)

  note <- cols[cols$column_name == "note", ]
  expect_equal(nrow(note), 1)
  expect_equal(note$begin_snapshot, ids$added)
  expect_true(is.na(note$end_snapshot))
})

test_that("view_table_changes returns an htmlwidget carrying the payload", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("htmlwidgets")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  build_audit_table()

  w <- view_table_changes(get_table_changes("audit"))
  expect_s3_class(w, "htmlwidget")
  expect_s3_class(w, "ducklake_changes")
  expect_equal(attr(w, "package"), "ducklake")
  expect_equal(w$x$title, "audit")
  expect_equal(w$x$counts$updated, 2L)

  # Feed columns stay arrays in the JSON even with one row, and data frames
  # serialize column-major, which is what the JavaScript reads
  one <- view_table_changes(
    get_table_changes("audit") |> dplyr::filter(change_type == "delete")
  )
  expect_s3_class(one$x$data$amount, "AsIs")
  json <- jsonlite::toJSON(one$x, dataframe = "columns", auto_unbox = TRUE, na = "null")
  expect_match(as.character(json), '"amount":\\["40"\\]')
  expect_match(as.character(json), '"kind":\\["delete"\\]')
})

test_that("view_table_changes needs htmlwidgets", {
  skip_if_not_installed("duckdb")

  df <- data.frame(snapshot_id = 1, rowid = 0, change_type = "insert", a = 1)
  local_mocked_bindings(requireNamespace = function(...) FALSE, .package = "base")
  expect_error(view_table_changes(df), "htmlwidgets")
})

test_that("build_changes_payload works on a plain data frame with no lake", {
  df <- data.frame(
    snapshot_id = c(2, 2, 1, 3),
    rowid = c(0, 0, 0, 1),
    change_type = c("update_postimage", "update_preimage", "insert", "delete"),
    a = c(2, 1, 1, 5),
    b = c("x", "x", "x", "y"),
    stringsAsFactors = FALSE
  )
  p <- build_changes_payload(df)
  expect_equal(p$items$kind, c("insert", "update", "delete"))
  expect_equal(p$items$snapshot_id, c(1, 2, 3))
  expect_equal(p$cells$column, "a")
  expect_equal(p$cells$old, "1")
  expect_equal(p$cells$new, "2")
  expect_equal(p$columns$changed_cells, c(1L, 0L))
  expect_equal(p$snapshots$snapshot_id, c(1, 2, 3))
  expect_false(p$schema$available)
  expect_equal(p$counts$rows_total, 4)
})

test_that("build_changes_payload rejects input that is not a change feed", {
  expect_error(build_changes_payload(data.frame(a = 1)), "snapshot_id")
  expect_error(build_changes_payload(42), "get_table_changes")
  df <- data.frame(snapshot_id = 1, rowid = 0, change_type = "insert", a = 1)
  expect_error(build_changes_payload(df, max_rows = 1), "max_rows")
  expect_error(build_changes_payload(df, max_rows = c(2, 3)), "max_rows")
  df$change_type <- "mystery"
  expect_warning(p <- build_changes_payload(df), "mystery")
  expect_equal(nrow(p$items), 0)
})

test_that("format_cell renders each column type as display text", {
  expect_equal(format_cell(c(TRUE, NA)), c("TRUE", NA))
  expect_equal(format_cell(c(1.5, NA, NaN)), c("1.5", NA, "NaN"))
  expect_equal(format_cell(c(3L, NA)), c("3", NA))
  expect_equal(format_cell(c("x", NA)), c("x", NA))
  expect_equal(format_cell(factor(c("a", NA))), c("a", NA))
  expect_equal(format_cell(as.Date(c("2026-01-02", NA))), c("2026-01-02", NA))
  expect_equal(
    format_cell(as.POSIXct(c("2026-01-02 03:04:05.5", NA), tz = "UTC")),
    c("2026-01-02 03:04:05.5", NA)
  )
  expect_equal(
    format_cell(as.POSIXct("2026-01-02 03:04:00", tz = "UTC")),
    "2026-01-02 03:04:00"
  )
  expect_equal(
    format_cell(as.difftime(c(47655, 0.5, NA), units = "secs")),
    c("13:14:15", "00:00:00.5", NA)
  )
  expect_equal(format_cell(list(as.raw(c(0x68, 0x69)), NULL)), c("6869", NA))
  expect_equal(format_cell(list(1:2, NULL, integer(0))), c("[1, 2]", NA, "[]"))
  expect_equal(
    format_cell(data.frame(a = c(1L, 2L), b = c("x", NA))),
    c("{a: 1, b: x}", "{a: 2, b: NA}")
  )

  expect_equal(simple_type_label(1L), "integer")
  expect_equal(simple_type_label(1.5), "number")
  expect_equal(simple_type_label(TRUE), "logical")
  expect_equal(simple_type_label("a"), "text")
  expect_equal(simple_type_label(factor("a")), "text")
  expect_equal(simple_type_label(Sys.Date()), "date")
  expect_equal(simple_type_label(Sys.time()), "timestamp")
  expect_equal(simple_type_label(as.difftime(1, units = "secs")), "time")
  expect_equal(simple_type_label(list(as.raw(1))), "blob")
  expect_equal(simple_type_label(list(1:2)), "list")
  expect_equal(simple_type_label(data.frame(a = 1)), "struct")
})
