# UNIT TESTS: table documentation (comments and labels)
#
# These tests verify set_table_comment(), set_column_comments(),
# get_table_comments(), create_table()'s label sync, and the
# collect() label restore.

test_that("table and column comments round trip through the catalog", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_comments")

  set_table_comment("test_comments", "A demo table")
  set_column_comments(
    "test_comments",
    id = "Record identifier",
    value = "Payload value"
  )

  comments <- get_table_comments("test_comments")
  expect_equal(nrow(comments), 3)
  tbl_row <- comments[comments$object_type == "table", ]
  expect_equal(tbl_row$comment, "A demo table")
  expect_true(is.na(tbl_row$column_name))
  col_rows <- comments[comments$object_type == "column", ]
  expect_setequal(col_rows$column_name, c("id", "value"))

  cleanup_temp_ducklake(lake)
})

test_that("multi-column comments land as one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(a = 1, b = 2, c = 3), "test_comment_snap")
  before <- max(list_table_snapshots()$snapshot_id)

  set_column_comments(
    "test_comment_snap",
    a = "first", b = "second", c = "third"
  )

  after <- max(list_table_snapshots()$snapshot_id)
  expect_equal(after - before, 1)

  cleanup_temp_ducklake(lake)
})

test_that("comments with special characters survive intact", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1), "test_comment_chars")

  tricky <- c(
    "subject's age (years); see 'protocol'",
    "unicode: µg/dL and em—dash",
    "injection-shaped: '; DROP TABLE test_comment_chars; --"
  )
  set_table_comment("test_comment_chars", tricky[1])
  expect_equal(
    get_table_comments("test_comment_chars")$comment, tricky[1]
  )
  set_table_comment("test_comment_chars", tricky[2])
  expect_equal(
    get_table_comments("test_comment_chars")$comment, tricky[2]
  )
  set_table_comment("test_comment_chars", tricky[3])
  expect_equal(
    get_table_comments("test_comment_chars")$comment, tricky[3]
  )
  # the table survived its own comment
  expect_equal(
    nrow(dplyr::collect(get_ducklake_table("test_comment_chars"))), 1
  )

  cleanup_temp_ducklake(lake)
})

test_that("NULL and NA clear comments", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1, value = "a"), "test_comment_clear")
  set_table_comment("test_comment_clear", "temporary")
  set_column_comments("test_comment_clear", id = "temp id", value = "kept")

  set_table_comment("test_comment_clear", NULL)
  set_column_comments("test_comment_clear", id = NA)

  comments <- get_table_comments("test_comment_clear")
  expect_equal(nrow(comments), 1)
  expect_equal(comments$column_name, "value")
  expect_equal(comments$comment, "kept")

  cleanup_temp_ducklake(lake)
})

test_that("get_table_comments filters by table and spans the lake without one", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1), "test_gc_one")
  create_table(data.frame(id = 1), "test_gc_two")
  set_table_comment("test_gc_one", "first table")
  set_table_comment("test_gc_two", "second table")

  all_comments <- get_table_comments()
  expect_setequal(
    all_comments$table_name, c("test_gc_one", "test_gc_two")
  )
  one <- get_table_comments("test_gc_one")
  expect_equal(one$comment, "first table")

  cleanup_temp_ducklake(lake)
})

test_that("create_table stores variable labels as column comments", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  df <- data.frame(id = 1:2, age = c(64, 71))
  attr(df$id, "label") <- "Subject ID"
  attr(df$age, "label") <- "Age (years)"

  expect_message(
    create_table(df, "test_labels"),
    "column labels"
  )
  comments <- get_table_comments("test_labels")
  expect_setequal(comments$comment, c("Subject ID", "Age (years)"))

  # creation plus labels is one snapshot
  snaps <- list_table_snapshots("test_labels")
  expect_equal(nrow(snaps), 1)

  cleanup_temp_ducklake(lake)
})

test_that("create_table with labels = FALSE and unlabelled frames stays quiet", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  df <- data.frame(id = 1:2)
  attr(df$id, "label") <- "Subject ID"
  create_table(df, "test_labels_off", labels = FALSE)
  expect_equal(nrow(get_table_comments("test_labels_off")), 0)

  expect_no_message(
    create_table(data.frame(x = 1), "test_labels_none")
  )
  expect_equal(nrow(get_table_comments("test_labels_none")), 0)

  cleanup_temp_ducklake(lake)
})

test_that("collect restores labels; derived columns stay unlabelled", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  df <- data.frame(id = 1:3, age = c(64, 71, 58))
  attr(df$age, "label") <- "Age (years)"
  create_table(df, "test_label_collect")

  out <- get_ducklake_table("test_label_collect") |> dplyr::collect()
  expect_equal(attr(out$age, "label"), "Age (years)")
  expect_null(attr(out$id, "label"))

  piped <- get_ducklake_table("test_label_collect") |>
    dplyr::mutate(older = age > 65) |>
    dplyr::collect()
  expect_equal(attr(piped$age, "label"), "Age (years)")
  expect_null(attr(piped$older, "label"))

  cleanup_temp_ducklake(lake)
})

test_that("collect leaves comment-free tables untouched", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_plain_collect")
  out <- get_ducklake_table("test_plain_collect") |> dplyr::collect()
  expect_null(attr(out$id, "label"))
  expect_equal(nrow(out), 2)

  cleanup_temp_ducklake(lake)
})

test_that("set_table_comment comments a view", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3), "test_comment_view_base")
  get_ducklake_table("test_comment_view_base") |>
    dplyr::filter(id > 1) |>
    create_view("v_commented")

  set_table_comment("v_commented", "Rows past the first")
  set_table_comment("test_comment_view_base", "The base table")

  comments <- get_table_comments()
  expect_equal(
    comments$comment[comments$object_type == "view"], "Rows past the first"
  )
  expect_equal(
    comments$comment[comments$object_type == "table"], "The base table"
  )

  cleanup_temp_ducklake(lake)
})

test_that("set_table_comment reaches a schema-qualified view and clears it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_schema("checks")
  create_table(data.frame(id = 1:3), "test_comment_view_schema")
  get_ducklake_table("test_comment_view_schema") |>
    dplyr::filter(id > 5) |>
    create_view("checks.id_in_range")

  set_table_comment("checks.id_in_range", "id is 5 or less")
  comments <- get_table_comments("checks.id_in_range")
  expect_equal(comments$object_type, "view")
  expect_equal(comments$schema_name, "checks")
  expect_equal(comments$comment, "id is 5 or less")

  set_table_comment("checks.id_in_range", NULL)
  expect_equal(nrow(get_table_comments("checks.id_in_range")), 0)

  cleanup_temp_ducklake(lake)
})

test_that("a view created and commented in one transaction is one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3), "test_comment_view_txn")
  before <- max(list_table_snapshots()$snapshot_id)

  # the view exists only inside the open transaction when it is commented
  suppressMessages(with_transaction({
    get_ducklake_table("test_comment_view_txn") |>
      dplyr::filter(id > 1) |>
      create_view("v_txn")
    set_table_comment("v_txn", "Commented before the commit")
  }))

  after <- max(list_table_snapshots()$snapshot_id)
  expect_equal(after - before, 1)
  expect_equal(
    get_table_comments("v_txn")$comment, "Commented before the commit"
  )

  cleanup_temp_ducklake(lake)
})

test_that("create_view keeps a view's comment across a replace, as one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_comment_view_repl")
  tricky <- "It's the subject's 'first' version; -- µg"
  suppressMessages({
    get_ducklake_table("test_comment_view_repl") |> create_view("v_repl_comment")
    set_table_comment("v_repl_comment", tricky)
  })
  before <- max(list_table_snapshots()$snapshot_id)

  suppressMessages(
    get_ducklake_table("test_comment_view_repl") |>
      dplyr::filter(id > 1) |>
      create_view("v_repl_comment")
  )

  # the new definition, the old comment, one snapshot for both
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_repl_comment"))), 2)
  expect_equal(get_table_comments("v_repl_comment")$comment, tricky)
  expect_equal(max(list_table_snapshots()$snapshot_id) - before, 1)

  # DuckLake on its own drops it: this is what create_view() works around
  DBI::dbExecute(
    lake$conn,
    "CREATE OR REPLACE VIEW v_repl_comment AS SELECT * FROM test_comment_view_repl"
  )
  expect_equal(nrow(get_table_comments("v_repl_comment")), 0)
})

test_that("create_view replaces an uncommented view without adding a comment", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_comment_view_plain")
  before <- max(list_table_snapshots()$snapshot_id)
  suppressMessages({
    get_ducklake_table("test_comment_view_plain") |> create_view("v_plain")
    get_ducklake_table("test_comment_view_plain") |>
      dplyr::filter(id > 1) |>
      create_view("v_plain")
  })

  expect_equal(nrow(get_table_comments("v_plain")), 0)
  expect_equal(max(list_table_snapshots()$snapshot_id) - before, 2)
  expect_false(ducklake:::in_transaction(lake$conn))
})

test_that("a replace inside a caller's transaction joins it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_comment_view_join")
  suppressMessages({
    get_ducklake_table("test_comment_view_join") |> create_view("v_join")
    set_table_comment("v_join", "Kept")
  })
  before <- max(list_table_snapshots()$snapshot_id)

  # rolled back: no commit of create_view()'s own slipped out
  suppressMessages({
    begin_transaction()
    get_ducklake_table("test_comment_view_join") |>
      dplyr::filter(id > 2) |>
      create_view("v_join")
    rollback_transaction()
  })
  expect_equal(max(list_table_snapshots()$snapshot_id), before)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_join"))), 3)
  expect_equal(get_table_comments("v_join")$comment, "Kept")

  # committed: a label set after the replace wins, in the same snapshot
  suppressMessages(with_transaction({
    get_ducklake_table("test_comment_view_join") |>
      dplyr::filter(id > 2) |>
      create_view("v_join")
    set_table_comment("v_join", "Reworded")
  }))
  expect_equal(max(list_table_snapshots()$snapshot_id) - before, 1)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_join"))), 1)
  expect_equal(get_table_comments("v_join")$comment, "Reworded")
})

test_that("a view created, commented, and replaced in one transaction keeps its comment", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_comment_view_local")
  suppressMessages(with_transaction({
    get_ducklake_table("test_comment_view_local") |> create_view("v_local")
    set_table_comment("v_local", "Set before the commit")
    get_ducklake_table("test_comment_view_local") |>
      dplyr::filter(id > 1) |>
      create_view("v_local")
  }))

  expect_equal(get_table_comments("v_local")$comment, "Set before the commit")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_local"))), 2)
})

test_that("get_table_comments follows a snapshot-pinned attach", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2, amount = c(5, 6)), "test_comment_pin")
  suppressMessages({
    set_table_comment("test_comment_pin", "table v1")
    set_column_comments("test_comment_pin", id = "id v1", amount = "amount v1")
  })
  pin <- max(list_table_snapshots()$snapshot_id)

  # afterwards: one comment reworded, one cleared, and a table that is new
  suppressMessages({
    set_table_comment("test_comment_pin", "table v2")
    set_column_comments("test_comment_pin", id = "id v2", amount = NA)
    create_table(data.frame(x = 1), "test_comment_pin_later")
    set_column_comments("test_comment_pin_later", x = "not there yet")
  })

  detach_ducklake(lake$ducklake_name)
  suppressMessages(attach_ducklake(
    lake$ducklake_name,
    lake_path = lake$lake_path,
    snapshot_version = pin
  ))

  comments <- get_table_comments()
  expect_setequal(comments$table_name, "test_comment_pin")
  expect_equal(
    comments$comment[comments$object_type == "table"], "table v1"
  )
  cols <- comments[comments$object_type == "column", ]
  expect_equal(cols$column_name, c("amount", "id"))
  expect_equal(cols$comment, c("amount v1", "id v1"))

  # collect() restores the labels of that snapshot, not today's
  pinned <- dplyr::collect(get_ducklake_table("test_comment_pin"))
  expect_equal(attr(pinned$id, "label"), "id v1")
  expect_equal(attr(pinned$amount, "label"), "amount v1")
})

test_that("get_table_comments sees comments pending in an open transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2), "test_comment_pending")

  seen <- NULL
  suppressMessages(with_transaction({
    set_table_comment("test_comment_pending", "Set before the commit")
    set_column_comments("test_comment_pending", id = "Identifier")
    seen <- get_table_comments("test_comment_pending")
  }))

  expect_equal(nrow(seen), 2)
  expect_setequal(seen$comment, c("Set before the commit", "Identifier"))
})

test_that("create_table from a query inherits a comment set in the same transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(with_transaction({
    create_table(data.frame(k = 1:2, v = c(5, 6)), "test_comment_txn_base")
    set_column_comments("test_comment_txn_base", v = "Value")
    get_ducklake_table("test_comment_txn_base") |>
      dplyr::filter(k > 0) |>
      create_table("test_comment_txn_derived")
  }))

  derived <- get_table_comments("test_comment_txn_derived")
  expect_equal(derived$column_name, "v")
  expect_equal(derived$comment, "Value")
})

test_that("set_column_comments validates its input", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1), "test_comment_valid")

  expect_error(
    set_column_comments("test_comment_valid"),
    "at least one"
  )
  expect_error(
    set_column_comments("test_comment_valid", "unnamed"),
    "named"
  )

  cleanup_temp_ducklake(lake)
})
