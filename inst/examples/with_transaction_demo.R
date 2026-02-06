# Demonstration of with_transaction() usage patterns
# This file shows the different ways to use with_transaction()

library(ducklake)
library(dplyr)

# Setup: Create a temporary data lake
install_ducklake()
temp_path <- tempdir()
attach_ducklake("demo_lake", lake_path = temp_path)

# ============================================================================
# Pattern 1: Single operation (most concise)
# ============================================================================

with_transaction(
  create_table(mtcars, "cars"),
  author = "Demo User",
  commit_message = "Add cars dataset"
)

# ============================================================================
# Pattern 2: Multiple operations in a block
# ============================================================================

with_transaction({
  create_table(iris, "flowers")
  create_table(airquality, "air_quality")
}, author = "Demo User", commit_message = "Add environmental datasets")

# ============================================================================
# Pattern 3: With dplyr pipeline (piped expression as argument)
# ============================================================================

with_transaction(
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>
    replace_table("cars"),
  author = "Demo User",
  commit_message = "Add km/L conversion"
)

# ============================================================================
# Pattern 4: Automatic rollback on error
# ============================================================================

tryCatch({
  with_transaction({
    create_table(ChickWeight, "chickens")
    stop("Simulated error")  # This will trigger rollback
  }, author = "Demo User", commit_message = "This won't be committed")
}, error = function(e) {
  message("Expected error - transaction was rolled back: ", e$message)
})

# Verify chickens table was NOT created (rollback worked)
tables <- DBI::dbListTables(get_ducklake_connection())
if ("chickens" %in% tables) {
  message("ERROR: Rollback failed!")
} else {
  message("SUCCESS: Rollback worked - chickens table does not exist")
}

# ============================================================================
# View transaction history
# ============================================================================

cat("\n=== Transaction History ===\n")
list_table_snapshots() |>
  select(table_name, snapshot_id, snapshot_time, changes) |>
  print()

# Cleanup
detach_ducklake()
cat("\nDemo complete!\n")
