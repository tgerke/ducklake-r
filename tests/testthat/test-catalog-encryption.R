# Tests for catalog encryption (META_ENCRYPTION_KEY)

test_that("build_attach_sql renders META_ENCRYPTION_KEY with escaping", {
  sql <- build_attach_sql(
    "lake", "/data", "duckdb", NULL,
    read_only = FALSE, meta_encryption_key = "k'x"
  )
  expect_match(sql, "META_ENCRYPTION_KEY 'k''x'", fixed = TRUE)

  # Absent when NULL
  sql2 <- build_attach_sql("lake", "/data", "duckdb", NULL, read_only = FALSE)
  expect_false(grepl("META_ENCRYPTION_KEY", sql2))
})

test_that("attach_ducklake validates meta_encryption_key", {
  expect_error(
    attach_ducklake(
      "enc_lake",
      lake_path = tempdir(),
      backend = "sqlite",
      catalog_connection_string = "metadata.sqlite",
      meta_encryption_key = "k"
    ),
    "duckdb"
  )
  expect_error(
    attach_ducklake("enc_lake", lake_path = tempdir(), meta_encryption_key = ""),
    "non-empty"
  )
  expect_error(
    attach_ducklake(
      "enc_lake",
      lake_path = tempdir(),
      meta_encryption_key = NA_character_
    ),
    "non-empty"
  )
  expect_error(
    attach_ducklake(
      "enc_lake",
      lake_path = tempdir(),
      meta_encryption_key = c("a", "b")
    ),
    "non-empty"
  )
})

test_that("a catalog encrypted with meta_encryption_key round-trips", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake_dir <- tempfile("test_enc_catalog")
  dir.create(lake_dir, showWarnings = FALSE, recursive = TRUE)

  tryCatch(
    {
      attach_ducklake(
        "vault_lake",
        lake_path = lake_dir,
        meta_encryption_key = "test-key"
      )
      create_table(mtcars, "cars")
      detach_ducklake("vault_lake", shutdown = TRUE)

      # Without the key DuckDB refuses to open the catalog; same for a
      # wrong key. The wording is DuckDB's, so no message assertions.
      expect_error(
        attach_ducklake("vault_lake", lake_path = lake_dir)
      )
      expect_error(
        attach_ducklake(
          "vault_lake",
          lake_path = lake_dir,
          meta_encryption_key = "wrong-key"
        )
      )

      attach_ducklake(
        "vault_lake",
        lake_path = lake_dir,
        meta_encryption_key = "test-key"
      )
      result <- get_ducklake_table("cars") |> dplyr::collect()
      expect_equal(nrow(result), 32)

      detach_ducklake("vault_lake", shutdown = TRUE)
    },
    finally = {
      unlink(lake_dir, recursive = TRUE)
    }
  )
})
