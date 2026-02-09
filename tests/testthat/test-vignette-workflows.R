# INTEGRATION TESTS: Vignette Workflows
#
# These are end-to-end integration tests that validate the complete workflows
# demonstrated in the package vignettes. They test that the advertised use cases
# work as documented, rather than testing individual low-level functions.
#
# Each test corresponds to a specific vignette:
# - ducklake.Rmd: Basic create/update workflow
# - time-travel.Rmd: Version history and time travel queries
# - clinical-trial-datalake.Rmd: Medallion architecture with related tables
# - transactions.Rmd: Manual transaction control with rollback
# - storage-and-backups.Rmd: Backup functionality

# Integration tests based on vignette workflows

# NOTE: This test runs first to ensure a clean connection state for file-based backup
test_that("storage-and-backups.Rmd workflow: backup and restore", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("fs")

  # Create separate temp directories for original and backup
  original_lake_dir <- file.path(tempdir(), "backup_test_original_first")
  backup_base_dir <- file.path(tempdir(), "backup_test_backups_first")
  dir.create(original_lake_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(backup_base_dir, showWarnings = FALSE, recursive = TRUE)

  tryCatch({
    # Set up ducklake in original location
    ducklake_name <- "backup_test_lake"
    attach_ducklake(ducklake_name, lake_path = original_lake_dir)

    # Create some data (convert Species to character to avoid ENUM)
    iris_df <- iris
    iris_df$Species <- as.character(iris_df$Species)

    with_transaction(
      create_table(iris_df, "iris_data"),
      author = "Data Engineer",
      commit_message = "Load iris dataset"
    )

    # Collect reference data from original ducklake
    original_data <- get_ducklake_table("iris_data") |> dplyr::collect()
    original_snapshots <- list_table_snapshots("iris_data")

    expect_equal(nrow(original_data), nrow(iris_df))
    expect_equal(nrow(original_snapshots), 1)

    # Perform actual backup using backup_ducklake()
    actual_backup_dir <- backup_ducklake(
      ducklake_name = ducklake_name,
      lake_path = original_lake_dir,
      backup_path = backup_base_dir
    )

    # Verify backup created both catalog and data files
    expect_true(dir.exists(actual_backup_dir))
    expect_true(file.exists(file.path(actual_backup_dir, paste0(ducklake_name, ".ducklake"))),
                info = "Catalog file should exist in backup")
    expect_true(dir.exists(file.path(actual_backup_dir, "main")),
                info = "Data directory should exist in backup")
    expect_true(dir.exists(file.path(actual_backup_dir, "main", "iris_data")),
                info = "Table data should exist in backup")

    # Detach original ducklake
    detach_ducklake(ducklake_name)

    # Restore from backup by attaching to backup location with SAME name (as shown in vignette)
    # The catalog internally remembers it's called "backup_test_lake"
    attach_ducklake(ducklake_name, lake_path = actual_backup_dir)

    # Verify the data is intact in the restored ducklake
    restored_data <- get_ducklake_table("iris_data") |> dplyr::collect()
    restored_snapshots <- list_table_snapshots("iris_data")

    # Compare data
    expect_equal(nrow(restored_data), nrow(original_data))
    expect_equal(ncol(restored_data), ncol(original_data))
    expect_true(all(names(original_data) %in% names(restored_data)))

    # Compare snapshot metadata
    expect_equal(nrow(restored_snapshots), nrow(original_snapshots))
    expect_equal(restored_snapshots$commit_message, original_snapshots$commit_message)
    expect_equal(restored_snapshots$author, original_snapshots$author)

    # Clean up - detach the restored ducklake
    detach_ducklake(ducklake_name)

  }, finally = {
    # Clean up directories
    unlink(original_lake_dir, recursive = TRUE)
    unlink(backup_base_dir, recursive = TRUE)
  })
})

test_that("ducklake.Rmd workflow: create lake, load data, update table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  # Create a data lake
  lake <- create_temp_ducklake()

  # Load data from a data.frame (as shown in vignette)
  with_transaction(
    create_table(mtcars, "cars"),
    author = "Data Engineer",
    commit_message = "Initial car data load"
  )

  # Verify table exists and has correct row count
  result <- get_ducklake_table("cars") |> dplyr::collect()
  expect_equal(nrow(result), nrow(mtcars))

  # Update an existing table (as shown in vignette)
  with_transaction(
    get_ducklake_table("cars") |>
      dplyr::mutate(kpl = mpg * 0.425144) |>
      replace_table("cars"),
    author = "Data Engineer",
    commit_message = "Add km/L metric"
  )

  # Verify the update worked
  result2 <- get_ducklake_table("cars") |> dplyr::collect()
  expect_true("kpl" %in% names(result2))
  expect_equal(nrow(result2), nrow(mtcars))

  # Verify we have 2 snapshots
  snapshots <- list_table_snapshots("cars")
  expect_equal(nrow(snapshots), 2)
  expect_true(all(c("snapshot_id", "snapshot_time", "changes") %in% names(snapshots)))

  cleanup_temp_ducklake(lake)
})

test_that("time-travel.Rmd workflow: version history and time travel queries", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Version 1: Initial load
  with_transaction(
    create_table(mtcars, "cars_timetravel"),
    author = "Data Engineer",
    commit_message = "Initial load of mtcars dataset"
  )

  # Version 2: Update fuel efficiency
  with_transaction(
    get_ducklake_table("cars_timetravel") |>
      dplyr::mutate(mpg = dplyr::if_else(hp > 200, mpg * 0.95, mpg)) |>
      replace_table("cars_timetravel"),
    author = "Data Analyst",
    commit_message = "Adjust MPG for high-performance vehicles"
  )

  # Version 3: Add classification
  with_transaction(
    get_ducklake_table("cars_timetravel") |>
      dplyr::mutate(
        efficiency_class = dplyr::case_when(
          mpg >= 25 ~ "High",
          mpg >= 20 ~ "Medium",
          TRUE ~ "Low"
        )
      ) |>
      replace_table("cars_timetravel"),
    author = "Data Analyst",
    commit_message = "Add efficiency classification"
  )

  # Test: Can we query historical versions?
  snapshots <- list_table_snapshots("cars_timetravel")
  expect_equal(nrow(snapshots), 3)

  # Query version 1 (original data without kpl or efficiency_class)
  v1_id <- snapshots$snapshot_id[1]  # Get actual snapshot ID
  v1 <- get_ducklake_table_version("cars_timetravel", version = v1_id) |> dplyr::collect()
  expect_false("efficiency_class" %in% names(v1))
  expect_equal(nrow(v1), nrow(mtcars))

  # Query version 3 (has efficiency_class)
  v3_id <- snapshots$snapshot_id[3]  # Get actual snapshot ID
  v3 <- get_ducklake_table_version("cars_timetravel", version = v3_id) |> dplyr::collect()
  expect_true("efficiency_class" %in% names(v3))
  expect_true(all(v3$efficiency_class %in% c("High", "Medium", "Low")))

  cleanup_temp_ducklake(lake)
})

test_that("clinical-trial-datalake.Rmd workflow: medallion architecture with multiple related tables", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Simulate Bronze layer: raw SDTM-like data
  dm_raw <- data.frame(
    USUBJID = paste0("001-", 1:10),
    AGE = c(25, 30, 45, 50, 55, 60, 65, 70, 75, 80),
    SEX = rep(c("M", "F"), 5),
    ARM = rep(c("Treatment", "Placebo"), 5),
    stringsAsFactors = FALSE
  )

  ae_raw <- data.frame(
    USUBJID = paste0("001-", c(1, 1, 3, 5, 7)),
    AETERM = c("Headache", "Nausea", "Fatigue", "Headache", "Dizziness"),
    AESEV = c("MILD", "MODERATE", "MILD", "MILD", "SEVERE"),
    stringsAsFactors = FALSE
  )

  # Load bronze layer
  with_transaction({
    create_table(dm_raw, "dm_raw")
    create_table(ae_raw, "ae_raw")
  },
  author = "Data Engineer",
  commit_message = "Load bronze layer SDTM data"
  )

  # Verify bronze layer loaded
  expect_equal(nrow(get_ducklake_table("dm_raw") |> dplyr::collect()), 10)
  expect_equal(nrow(get_ducklake_table("ae_raw") |> dplyr::collect()), 5)

  # Silver layer: cleaned data
  with_transaction(
    get_ducklake_table("dm_raw") |>
      dplyr::mutate(age_group = dplyr::case_when(
        AGE < 40 ~ "Young",
        AGE < 65 ~ "Middle",
        TRUE ~ "Senior"
      )) |>
      create_table("dm_clean"),
    author = "Statistician",
    commit_message = "Create cleaned DM with age groups"
  )

  # Gold layer: Analysis dataset (like ADaM) - join DM and AE
  with_transaction(
    get_ducklake_table("dm_clean") |>
      dplyr::left_join(
        get_ducklake_table("ae_raw"),
        by = "USUBJID"
      ) |>
      create_table("adsl"),
    author = "Statistician",
    commit_message = "Create ADSL analysis dataset"
  )

  # Verify the relational join worked
  adsl <- get_ducklake_table("adsl") |> dplyr::collect()
  expect_true(nrow(adsl) >= nrow(dm_raw))  # Should have at least as many rows due to left join
  expect_true(all(c("USUBJID", "AGE", "age_group", "AETERM") %in% names(adsl)))

  # Verify we can track snapshots across all tables
  all_snapshots <- list_table_snapshots()
  expect_true(nrow(all_snapshots) >= 3)  # At least 3 tables created

  cleanup_temp_ducklake(lake)
})

test_that("transactions.Rmd workflow: manual transaction control with rollback", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Setup: create initial table
  create_table(mtcars[1:10, ], "cars_txn")

  # Test successful transaction
  begin_transaction()
  get_ducklake_table("cars_txn") |>
    dplyr::mutate(mpg_doubled = mpg * 2) |>
    replace_table("cars_txn")
  commit_transaction(
    author = "Analyst",
    commit_message = "Add doubled MPG"
  )

  result <- get_ducklake_table("cars_txn") |> dplyr::collect()
  expect_true("mpg_doubled" %in% names(result))

  # Test rollback
  begin_transaction()
  get_ducklake_table("cars_txn") |>
    dplyr::mutate(should_not_exist = 999) |>
    replace_table("cars_txn")
  rollback_transaction()

  result2 <- get_ducklake_table("cars_txn") |> dplyr::collect()
  expect_false("should_not_exist" %in% names(result2))
  expect_true("mpg_doubled" %in% names(result2))  # Still has committed changes

  cleanup_temp_ducklake(lake)
})

test_that("package provides dplyr-like interface for complex queries", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(mtcars, "cars_dplyr")

  # Test that standard dplyr verbs work seamlessly
  result <- get_ducklake_table("cars_dplyr") |>
    dplyr::filter(mpg > 20) |>
    dplyr::group_by(cyl) |>
    dplyr::summarise(
      n = dplyr::n(),
      avg_mpg = mean(mpg, na.rm = TRUE),
      avg_hp = mean(hp, na.rm = TRUE)
    ) |>
    dplyr::arrange(dplyr::desc(avg_mpg)) |>
    dplyr::collect()

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(all(c("cyl", "n", "avg_mpg", "avg_hp") %in% names(result)))

  cleanup_temp_ducklake(lake)
})
