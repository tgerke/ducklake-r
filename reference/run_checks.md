# Run the data checks stored in a schema

**Experimental.** Counts the rows returned by each view in a schema set
aside for data checks. The convention: a check is a view that returns
the rows breaking a rule, the view's name is the rule's id, and its
comment is the rule's label. Checks written this way live in the
DuckLake catalog, so they are versioned with the data and every client
of the lake can run them. The interface may change while the convention
settles.

## Usage

``` r
run_checks(schema_name = "checks", ducklake_name = NULL)
```

## Arguments

- schema_name:

  The schema that holds the check views (default `"checks"`).

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per check, ordered by name: `check` (the view
name), `label` (the view's comment, `NA` without one), and `n_fail` (the
number of rows the view returns). Zero rows when the schema holds no
views.

## Details

Write a check with
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
from a pipeline that keeps the failing rows, such as
`filter(!(cyl %in% c(4, 6, 8)))`, and label it with
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md).
`NOT (condition)` is not true for a row where the condition is `NA`, so
a missing value passes unless a check of its own looks for it. Read the
table by its schema-qualified name, `get_ducklake_table("main.cars")`:
the view then binds whichever database is current, for this function's
`ducklake_name` and for other clients of the lake.

Every view in the schema counts as a check, so keep other views
elsewhere. A view that summarizes, returning a row of totals, reports a
failure every time. A check whose view no longer binds, after a column
rename for instance, is an error and not a pass; DuckDB's message quotes
the line naming the view.

Inside
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
the counts include the transaction's pending writes, so a load can be
checked before it commits: [`stop()`](https://rdrr.io/r/base/stop.html)
when a check fails and the transaction rolls back. On a lake attached
with `snapshot_version` or `snapshot_time`, the checks and the data are
both read as of that snapshot.

## See also

[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
and
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md)
to write a check,
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
to gate a load on the result, and
[`vignette("data-checks")`](https://tgerke.github.io/ducklake-r/articles/data-checks.md).

## Examples

``` r
lake_dir <- tempfile("checks_lake_")
dir.create(lake_dir)
attach_ducklake("checks_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# A check is a view of the rows that break a rule
create_schema("checks")
#> Created schema "checks".
get_ducklake_table("main.cars") |>
  dplyr::filter(!(cyl %in% c(4, 6, 8))) |>
  create_view("checks.cyl_known")
#> Created view "checks.cyl_known".
set_table_comment("checks.cyl_known", "cyl is 4, 6, or 8")
#> Commented view "checks.cyl_known".

run_checks()
#>       check             label n_fail
#> 1 cyl_known cyl is 4, 6, or 8      0

detach_ducklake("checks_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
