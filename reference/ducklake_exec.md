# Execute DuckLake operations from dplyr queries

Execute DuckLake operations from dplyr queries

## Usage

``` r
ducklake_exec(.data, table_name = NULL, .quiet = TRUE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with accumulated operations

- table_name:

  The target table name for the operation. If not provided, will be
  extracted from the table attribute (set by get_ducklake_table())

- .quiet:

  Logical, whether to suppress debug output (default TRUE)

## Value

The result from duckplyr::db_exec()

## Details

This function automatically detects the type of operation based on dplyr
verbs:

- Filter-only queries generate DELETE operations (removes rows that
  DON'T match filter)

- Queries with mutate() generate UPDATE operations

- Other queries generate INSERT operations

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete rows that don't match filter (table name inferred)
get_ducklake_table("my_table") |>
  filter(status == "inactive") |>
  ducklake_exec()

# Update specific rows (table name inferred)
get_ducklake_table("my_table") |>
  filter(id == 123) |>
  mutate(status = "updated") |>
  ducklake_exec()

# Or provide table name explicitly
tbl(con, "my_table") |>
  select(id, name) |>
  mutate(computed_field = name * 2) |>
  ducklake_exec("my_table")
} # }
```
