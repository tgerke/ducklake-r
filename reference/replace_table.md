# Replace a table with modified data and create a new snapshot

Replace a table with modified data and create a new snapshot

## Usage

``` r
replace_table(.data, table_name, .quiet = TRUE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with transformations

- table_name:

  Table name to replace

- .quiet:

  Logical, whether to suppress messages (default TRUE)

## Value

Invisibly returns NULL

## Details

This function is designed for schema changes or bulk transformations
that should create a new versioned snapshot. It:

1.  Collects the transformed data

2.  Drops the existing table

3.  Creates a new table with the updated schema/data

All operations happen within the current transaction context. Use
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
and
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
to ensure proper versioning.

**When to use replace_table():**

- **Adding new columns** - DuckLake UPDATE cannot add columns; use
  replace_table()

- **Removing columns** - Restructure schema with select()

- **Versioning needed** - Creates snapshots via DROP + CREATE for time
  travel

- **Complex transformations** - Apply full dplyr pipelines naturally

**When to use update_table() instead:**

- Modifying existing column values only (no schema changes)

- Performance critical and versioning not needed

- Making targeted corrections to specific rows

## Examples

``` r
if (FALSE) { # \dontrun{
# Add new derived columns with versioning
begin_transaction()
get_ducklake_table("adsl") |>
  mutate(
    AGE65FL = if_else(AGE >= 65, "Y", "N"),
    AGECAT = case_when(
      AGE < 65 ~ "<65",
      AGE >= 65 & AGE < 75 ~ "65-74",
      AGE >= 75 ~ ">=75"
    )
  ) |>
  replace_table("adsl")
commit_transaction()

# Remove columns and create new snapshot
begin_transaction()
get_ducklake_table("adsl") |>
  select(-AGE65FL, -AGECAT) |>
  replace_table("adsl")
commit_transaction()
} # }
```
