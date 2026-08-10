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
