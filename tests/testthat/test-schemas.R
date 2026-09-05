# Schemas and schema-qualified table names

test_that("create_schema() and drop_schema() manage schemas in the current lake", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_message(create_schema("bronze"), "Created schema")
  expect_message(create_schema("bronze"), "already exists")
  expect_error(create_schema("bronze", if_not_exists = FALSE), "already exists")

  suppressMessages(create_table(mtcars, "bronze.cars"))
  tables <- list_ducklake_tables()
  expect_true(any(tables$schema_name == "bronze" & tables$table_name == "cars"))

  expect_error(drop_schema("bronze"), "depend")
  expect_message(drop_schema("bronze", cascade = TRUE), "Dropped schema")
  expect_false(any(list_ducklake_tables()$schema_name == "bronze"))
})

test_that("schema-qualified names work across the write paths", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(create_schema("bronze"))
  df <- data.frame(id = 1:4, grp = c("a", "b", "a", "b"), v = c(10, 20, 30, 40))
  attr(df$v, "label") <- "Value"
  suppressMessages(create_table(df, "bronze.t"))

  x <- get_ducklake_table("bronze.t")
  expect_s3_class(x, "tbl_ducklake")
  expect_equal(attr(x, "ducklake_table_name"), "bronze.t")
  expect_equal(nrow(dplyr::collect(x)), 4)
  expect_equal(attr(dplyr::collect(x)$v, "label"), "Value")

  suppressMessages({
    rows_insert(x, data.frame(id = 5L, grp = "a", v = 50), by = "id")
    rows_update(x, data.frame(id = 1L, v = 11), by = "id")
    rows_upsert(x, data.frame(id = c(2L, 6L), v = c(22, 60)), by = "id")
    rows_delete(x, data.frame(id = 3L), by = "id")
    merge_into("bronze.t", data.frame(id = 6L, grp = "b", v = 61), by = "id")
    x |>
      dplyr::filter(id == 4) |>
      dplyr::mutate(v = 44) |>
      ducklake_exec()
  })

  result <- dplyr::collect(x) |> dplyr::arrange(id)
  expect_equal(result$id, c(1L, 2L, 4L, 5L, 6L))
  expect_equal(as.numeric(result$v), c(11, 22, 44, 50, 61))

  # A lazy pipeline on a schema table writes into another schema in-database
  suppressMessages({
    create_schema("silver")
    x |> dplyr::filter(v > 20) |> create_table("silver.t")
  })
  expect_equal(nrow(dplyr::collect(get_ducklake_table("silver.t"))), 4)
  expect_equal(attr(dplyr::collect(get_ducklake_table("silver.t"))$v, "label"), "Value")
})

test_that("schema-qualified names work across the metadata readers", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages({
    create_schema("bronze")
    create_table(data.frame(id = 1:4, grp = c("a", "b", "a", "b")), "bronze.t")
    create_table(data.frame(id = 1:2), "t")
    set_table_comment("bronze.t", "Bronze copy")
    set_column_comments("bronze.t", grp = "Group")
    set_table_comment("t", "Main copy")
    set_table_partitioning("bronze.t", "grp")
    set_table_sorting("bronze.t", "id DESC")
    set_ducklake_option("parquet_compression", "zstd", table_name = "bronze.t")
    rows_insert(get_ducklake_table("bronze.t"), data.frame(id = 5L, grp = "a"), by = "id")
  })

  comments <- get_table_comments("bronze.t")
  expect_equal(unique(comments$schema_name), "bronze")
  expect_equal(comments$comment[comments$object_type == "table"], "Bronze copy")
  expect_equal(comments$comment[comments$column_name %in% "grp"], "Group")
  # A bare name matches the table in every schema
  both <- get_table_comments("t")
  expect_setequal(both$schema_name[both$object_type == "table"], c("bronze", "main"))

  partitions <- get_table_partitions("bronze.t")
  expect_equal(partitions$schema_name, "bronze")
  expect_equal(partitions$column_name, "grp")
  expect_equal(get_table_sorting("bronze.t")$expression, "id")
  expect_equal(nrow(get_table_partitions("t")), 1)

  opts <- get_ducklake_options(lake$ducklake_name)
  expect_true(any(
    opts$scope == "TABLE" & opts$scope_entry %in% "bronze.t" &
      opts$option_name == "parquet_compression"
  ))
  suppressMessages(set_inlining_row_limit(25, table_name = "bronze.t"))
  expect_equal(get_inlining_row_limit(table_name = "bronze.t"), 25L)

  snapshots <- list_table_snapshots("bronze.t")
  expect_gte(nrow(snapshots), 2)
  changes <- get_table_changes(
    "bronze.t", min(snapshots$snapshot_id), max(snapshots$snapshot_id)
  ) |>
    dplyr::collect()
  expect_true("insert" %in% changes$change_type)
  expect_equal(nrow(changes), 5)

  info <- get_table_info("bronze.t")
  expect_equal(nrow(info), 1)
  expect_equal(info$schema_name, "bronze")
  expect_equal(nrow(get_table_info("t")), 2)

  expect_s3_class(flush_inlined_data(table_name = "bronze.t"), "data.frame")
  expect_gte(nrow(list_ducklake_files("bronze.t")), 1)
  expect_error(list_ducklake_files("bronze.t", schema_name = "silver"), "names schema")
  expect_s3_class(merge_adjacent_files(table_name = "bronze.t"), "data.frame")
  expect_s3_class(rewrite_data_files(table_name = "bronze.t"), "data.frame")

  # Rewrites and restores keep the schema table's metadata
  first <- min(list_table_snapshots("bronze.t")$snapshot_id)
  suppressMessages(
    get_ducklake_table("bronze.t") |>
      dplyr::filter(id > 1) |>
      replace_table("bronze.t")
  )
  expect_equal(get_table_partitions("bronze.t")$column_name, "grp")
  comments <- get_table_comments("bronze.t")
  expect_equal(comments$comment[comments$object_type == "table"], "Bronze copy")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("bronze.t"))), 4)

  expect_equal(nrow(dplyr::collect(get_ducklake_table_version("bronze.t", first))), 4)
  suppressMessages(restore_table_version("bronze.t", version = first))
  expect_equal(sort(dplyr::collect(get_ducklake_table("bronze.t"))$id), 1:4)
  expect_equal(get_table_partitions("bronze.t")$column_name, "grp")
})
