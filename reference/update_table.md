# Update existing column values in a table (in-place, no versioning)

Update existing column values in a table (in-place, no versioning)

## Usage

``` r
update_table(.data, table_name, .quiet = FALSE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with mutate() operations

- table_name:

  Table name to update

- .quiet:

  Logical, whether to suppress debug output (default FALSE for backward
  compatibility)

## Value

Invisibly returns the SQL statement string after executing it

## Details

This function performs in-place UPDATE operations on existing columns.
**Important limitations:**

- **Cannot add or remove columns** - Only modifies values in existing
  columns

- **Does not create snapshots** - UPDATE operations modify in-place
  without creating snapshots, even when wrapped in transactions. Only
  CREATE operations trigger snapshots.

- **All columns must exist** - Any column referenced in mutate() must
  already exist in the table

Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
if you need to:

- Add new derived columns

- Remove columns

- Create a new versioned snapshot

Use `update_table()` when:

- Making targeted value corrections to existing columns

- Performance is critical and versioning is not needed

- Updating specific rows with filter()

## Examples

``` r
if (FALSE) n#' # Correct a specific value (no versioning needed)
get_ducklake_table("adsl") |>
  mutate(SAFFL = if_else(USUBJID == "01-701-1015", "N", SAFFL)) |>
  update_table("adsl")
#> === DEBUG: update_table called ===
#> Error in mutate(get_ducklake_table("adsl"), SAFFL = if_else(USUBJID ==     "01-701-1015", "N", SAFFL)): could not find function "mutate"

# Update multiple columns
get_ducklake_table("adae") |>
  mutate(
    AESEV = if_else(AESEV == "MILD", "MODERATE", AESEV),
    AESER = if_else(AESEV == "SEVERE", "Y", AESER)
  ) |>
  update_table("adae")
#> === DEBUG: update_table called ===
#> Error in mutate(get_ducklake_table("adae"), AESEV = if_else(AESEV == "MILD",     "MODERATE", AESEV), AESER = if_else(AESEV == "SEVERE", "Y",     AESER)): could not find function "mutate"
 # \dontrun{}
```
