# UNIT TESTS: create_check() and run_checks()
#
# A check is a view that returns the rows breaking a rule. These tests cover
# writing one from the rule as stated, the counts and labels, the empty and
# missing schema, quoting, lake scope, and the behaviors the pattern rests
# on: a gate inside a transaction, a rebuilt base table, and a
# snapshot-pinned attach.

# Two checks on one table: `amount_positive` is labelled, `id_present` is
# not. Tables are read by schema-qualified name so the views bind whichever
# database is current.
create_test_checks <- function(table_name) {
  table_name <- paste0("main.", table_name)
  create_schema("checks")
  get_ducklake_table(table_name) |>
    dplyr::filter(!(amount > 0)) |>
    create_view("checks.amount_positive")
  set_table_comment("checks.amount_positive", "amount is above zero")
  get_ducklake_table(table_name) |>
    dplyr::filter(is.na(id)) |>
    create_view("checks.id_present")
}

test_that("create_check takes the rule as stated and keeps the rows that break it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(
    data.frame(id = 1:5, dose = c(10, 0, NA, 5, 0), site = c("a", "b", "a", "b", "a")),
    "test_cc_base"
  )
  before <- max(list_table_snapshots()$snapshot_id)

  # a prohibition, stated as it is said: no double negative to write
  expect_message(
    get_ducklake_table("main.test_cc_base") |>
      create_check("dose_not_zero", dose != 0, label = "dose is not 0", listing = c(id, dose)),
    "dose_not_zero"
  )

  # the schema, the view, and the label are one snapshot
  expect_equal(max(list_table_snapshots()$snapshot_id) - before, 1)
  expect_false(ducklake:::in_transaction(lake$conn))

  result <- run_checks()
  expect_equal(result$check, "dose_not_zero")
  expect_equal(result$label, "dose is not 0")
  expect_equal(result$n_fail, 2)

  # the listing, and a missing dose passes
  failing <- dplyr::collect(get_ducklake_table("checks.dose_not_zero"))
  expect_named(failing, c("id", "dose"))
  expect_equal(sort(failing$id), c(2, 5))
})

test_that("create_check selects the same rows as the failure written by hand", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(
    data.frame(id = 1:6, a = c(1, -1, NA, 2, NA, -3), b = c(1, 1, 1, NA, -1, NA)),
    "test_cc_logic"
  )
  suppressMessages({
    get_ducklake_table("main.test_cc_logic") |>
      create_check("both_positive", a > 0 & b > 0, label = "a and b are above zero")
    # De Morgan by hand, under three-valued logic
    get_ducklake_table("main.test_cc_logic") |>
      dplyr::filter(a <= 0 | b <= 0) |>
      create_view("checks.both_positive_by_hand")
  })

  stated <- dplyr::pull(get_ducklake_table("checks.both_positive"), id)
  by_hand <- dplyr::pull(get_ducklake_table("checks.both_positive_by_hand"), id)
  expect_equal(sort(stated), sort(by_hand))
  expect_equal(sort(stated), c(2, 5, 6))
})

test_that("create_check joins a transaction and revises a check in one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:4, hp = c(30, 90, 450, 120)), "test_cc_revise")
  suppressMessages(
    get_ducklake_table("main.test_cc_revise") |>
      create_check("hp_plausible", hp > 0 & hp < 500, label = "hp is between 0 and 500")
  )
  expect_equal(run_checks()$n_fail, 0)
  before <- max(list_table_snapshots()$snapshot_id)

  suppressMessages(with_transaction(
    get_ducklake_table("main.test_cc_revise") |>
      create_check("hp_plausible", hp >= 40 & hp <= 400, label = "hp is between 40 and 400"),
    author = "Data Manager", commit_message = "Narrow the plausible hp range"
  ))

  expect_equal(max(list_table_snapshots()$snapshot_id) - before, 1)
  snapshot <- list_table_snapshots()
  expect_equal(snapshot$author[nrow(snapshot)], "Data Manager")
  result <- run_checks()
  expect_equal(result$label, "hp is between 40 and 400")
  expect_equal(result$n_fail, 2)

  # replace = FALSE refuses an existing check, and rolls its transaction back
  expect_error(
    suppressMessages(
      get_ducklake_table("main.test_cc_revise") |>
        create_check("hp_plausible", hp > 0, label = "hp is positive", replace = FALSE)
    )
  )
  expect_false(ducklake:::in_transaction(lake$conn))
  expect_equal(run_checks()$label, "hp is between 40 and 400")
})

test_that("create_check resolves its schema and validates its input", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_cc_schema")
  tbl <- get_ducklake_table("main.test_cc_schema")
  suppressMessages({
    create_check(tbl, "qc.id_small", id < 3, label = "id is below 3")
    create_check(tbl, "ID under 3; DROP TABLE x", id < 3, label = "quoted", schema_name = "Edit Checks")
  })
  expect_equal(run_checks("qc")$check, "id_small")
  expect_equal(run_checks("Edit Checks")$check, "ID under 3; DROP TABLE x")
  expect_equal(run_checks("Edit Checks")$n_fail, 1)

  expect_error(
    create_check(tbl, "qc.id_small", id < 3, label = "x", schema_name = "checks"),
    "names schema"
  )
  expect_error(create_check(tbl, "no_label", id < 3), "label")
  expect_error(create_check(tbl, "empty_label", id < 3, label = ""), "label")
  expect_error(
    create_check(data.frame(id = 1), "from_df", id < 3, label = "x"),
    "lazy table"
  )
})

test_that("run_checks counts failing rows and reads labels", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  # an NA amount breaks no rule: NOT (NA > 0) is not true
  create_table(
    data.frame(id = 1:5, amount = c(10, -1, 0, NA, 3)),
    "test_checks_base"
  )
  suppressMessages(create_test_checks("test_checks_base"))

  result <- run_checks()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("check", "label", "n_fail"))
  expect_equal(result$check, c("amount_positive", "id_present"))
  expect_equal(result$label, c("amount is above zero", NA))
  expect_equal(result$n_fail, c(2, 0))
})

test_that("run_checks returns a typed zero-row frame for an empty schema", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(create_schema("checks"))

  result <- run_checks()
  expect_equal(nrow(result), 0)
  expect_named(result, c("check", "label", "n_fail"))
  expect_type(result$check, "character")
  expect_type(result$label, "character")
  expect_type(result$n_fail, "double")
})

test_that("run_checks refuses a schema that does not exist", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_error(run_checks(), "does not exist")
  expect_error(run_checks("chekcs"), "does not exist")
})

test_that("run_checks quotes awkward schema and view names", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "test_checks_quote")
  suppressMessages({
    create_schema("Edit Checks")
    get_ducklake_table("main.test_checks_quote") |>
      dplyr::filter(id > 2) |>
      create_view("Edit Checks.ID under 3; DROP TABLE x")
  })

  result <- run_checks("Edit Checks")
  expect_equal(result$check, "ID under 3; DROP TABLE x")
  expect_equal(result$n_fail, 1)
})

test_that("run_checks scopes to one lake among several", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake_one <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake_one), add = TRUE)
  create_table(data.frame(id = 1:3), "test_checks_one")
  suppressMessages({
    create_schema("checks")
    get_ducklake_table("main.test_checks_one") |>
      dplyr::filter(id > 1) |>
      create_view("checks.only_in_one")
  })

  lake_two <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake_two), add = TRUE, after = FALSE)
  create_table(data.frame(id = 1:3), "test_checks_two")
  suppressMessages({
    create_schema("checks")
    get_ducklake_table("main.test_checks_two") |>
      dplyr::filter(id > 2) |>
      create_view("checks.only_in_two")
  })

  # the second lake is current: default runs its checks, the name reaches back
  expect_equal(run_checks()$check, "only_in_two")
  expect_equal(run_checks()$n_fail, 1)
  one <- run_checks(ducklake_name = lake_one$ducklake_name)
  expect_equal(one$check, "only_in_one")
  expect_equal(one$n_fail, 2)
})

test_that("a failing gate inside with_transaction commits nothing", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "test_checks_gate")
  suppressMessages(create_test_checks("test_checks_gate"))
  expect_equal(run_checks()$n_fail, c(0, 0))
  before <- max(list_table_snapshots()$snapshot_id)

  seen <- NULL
  expect_error(
    suppressMessages(with_transaction({
      rows_insert(
        get_ducklake_table("test_checks_gate"),
        data.frame(id = 4L, amount = -5),
        by = "id"
      )
      seen <- run_checks()$n_fail
      if (any(seen > 0)) stop("checks failed")
    })),
    "rolled back"
  )

  # the pending row was counted, and nothing of the load remains
  expect_equal(seen, c(1, 0))
  expect_equal(max(list_table_snapshots()$snapshot_id), before)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("test_checks_gate"))), 3)
  expect_equal(run_checks()$n_fail, c(0, 0))
})

test_that("a view created in the open transaction is run with its label", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, amount = c(10, -2, 30)), "test_checks_txn")

  seen <- NULL
  suppressMessages(with_transaction({
    create_test_checks("test_checks_txn")
    seen <- run_checks()
  }))

  expect_equal(seen$label, c("amount is above zero", NA))
  expect_equal(seen$n_fail, c(1, 0))
})

test_that("check views survive replace_table() on the table they read", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, amount = c(10, -2, 30)), "test_checks_repl")
  suppressMessages(create_test_checks("test_checks_repl"))
  expect_equal(run_checks()$n_fail, c(1, 0))

  suppressMessages(
    get_ducklake_table("test_checks_repl") |>
      dplyr::mutate(amount = abs(amount)) |>
      replace_table("test_checks_repl")
  )

  result <- run_checks()
  expect_equal(result$n_fail, c(0, 0))
  expect_equal(result$label[[1]], "amount is above zero")
})

test_that("run_checks on a pinned attach runs that snapshot's rules on its data", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, amount = c(10, -2, 30)), "test_checks_pin")
  suppressMessages(create_test_checks("test_checks_pin"))
  pin <- max(list_table_snapshots()$snapshot_id)

  # afterwards: the bad row is fixed, a rule is dropped, a rule is relabelled
  suppressMessages({
    rows_update(
      get_ducklake_table("test_checks_pin"),
      data.frame(id = 2L, amount = 2),
      by = "id"
    )
    drop_view("checks.id_present")
    set_table_comment("checks.amount_positive", "amount exceeds zero")
  })
  now <- run_checks()
  expect_equal(now$check, "amount_positive")
  expect_equal(now$label, "amount exceeds zero")
  expect_equal(now$n_fail, 0)

  detach_ducklake(lake$ducklake_name)
  suppressMessages(attach_ducklake(
    lake$ducklake_name,
    lake_path = lake$lake_path,
    snapshot_version = pin
  ))

  then <- run_checks()
  expect_equal(then$check, c("amount_positive", "id_present"))
  expect_equal(then$label, c("amount is above zero", NA))
  expect_equal(then$n_fail, c(1, 0))
})
