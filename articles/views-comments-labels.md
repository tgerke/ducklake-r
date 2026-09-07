# Views, Comments, and Labels

``` r

library(ducklake)
library(dplyr)

attach_ducklake("views_lake", lake_path = vignette_temp_dir)

with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Add the Motor Trend car data"
)
```

Tables hold data. A lake also holds three kinds of things about the
data: views, which store a query; comments, which document a table or a
column; and variable labels, which the lake stores as comments. All
three live in the catalog, so they travel with the lake and every client
of it can read them.

## Views

A view is a stored query. Reading it runs the query against the tables
as they are now, so a view is never stale.
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
takes a dplyr pipeline built on lake tables and stores its SQL. Like any
change to the lake, it can be committed with an author and a message:

``` r

with_transaction(
  get_ducklake_table("cars") |>
    filter(mpg > 25) |>
    select(mpg, cyl, wt) |>
    create_view("v_efficient_cars"),
  author = "Data Engineer",
  commit_message = "Define the efficient cars everyone reports on"
)
#> Created view "v_efficient_cars".
#> Committed snapshot 2 (Data Engineer): Define the efficient cars everyone
#> reports on

get_ducklake_table("v_efficient_cars") |> collect()
#> # A tibble: 6 × 3
#>     mpg   cyl    wt
#>   <dbl> <dbl> <dbl>
#> 1  32.4     4  2.2 
#> 2  30.4     4  1.62
#> 3  33.9     4  1.84
#> 4  27.3     4  1.94
#> 5  26       4  2.14
#> 6  30.4     4  1.51
```

[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
reads views and tables alike, and further verbs pipe onto a view as they
would onto a table:

``` r

get_ducklake_table("v_efficient_cars") |>
  count(cyl)
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpTybTNg/ducklake/ducklake2f26543ff3ec.duckdb]
#>     cyl     n
#>   <dbl> <dbl>
#> 1     4     6
```

Views are versioned. Creating, replacing, or dropping one is a snapshot
in the lake’s history, like any change to a table:

``` r

list_table_snapshots("v_efficient_cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           2 2026-09-07 18:17:29              2
#>                                changes        author
#> 1 views_created, main.v_efficient_cars Data Engineer
#>                                  commit_message commit_extra_info
#> 1 Define the efficient cars everyone reports on              <NA>
```

### When a view is the right tool

Reach for a view when the logic belongs to the lake rather than to you:
a team’s shared definition of an analysis population, a filter every
report must apply, or a derived layer that is cheap enough to compute on
read and so need not be stored. Every client sees the same definition,
whether it connects from R, Python, or plain SQL, and a change to the
definition is recorded in the snapshot history.

Two cases call for something else. An exploratory pipeline that only you
use is better kept in your R script under version control, where it can
change freely without adding snapshots to a shared lake. And a result
that should be materialized, because it is expensive to compute or
because downstream users need a fixed dataset, belongs in a table:
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
or
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
on the same pipeline.

Drop a view when its logic is no longer needed:

``` r

drop_view("v_efficient_cars")
#> Dropped view "v_efficient_cars".
```

For logic a view cannot hold, such as a parameterized SQL macro, DuckDB
SQL is the escape hatch:
`DBI::dbExecute(get_ducklake_connection(), "CREATE MACRO ...")`.

## Table and column comments

Comments live in the catalog, so the documentation travels with the data
instead of in a sidecar file:

``` r

set_table_comment("cars", "Motor Trend road tests of 1973-74 models")
#> Commented table "cars".
set_column_comments(
  "cars",
  mpg = "Miles per US gallon",
  wt = "Weight (1000 lbs)"
)
#> Commented 2 columns on "cars".

get_table_comments("cars")
#>   object_type schema_name table_name column_name
#> 1      column        main       cars         mpg
#> 2      column        main       cars          wt
#> 3       table        main       cars        <NA>
#>                                    comment
#> 1                      Miles per US gallon
#> 2                        Weight (1000 lbs)
#> 3 Motor Trend road tests of 1973-74 models
```

## Variable labels

If your data carries variable labels, they survive the lake.
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
stores each column’s `label` attribute as a column comment, and
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html) puts
the attribute back, so label-aware tools such as gtsummary and gt behave
as if the data never left R. Data imported with haven from SAS, SPSS, or
Stata arrives with these labels already. For a data frame built in R,
the labelled package sets them:

``` r

library(labelled)

df_visits <- data.frame(subject = c("S1", "S2"), sbp = c(128, 141)) |>
  set_variable_labels(
    subject = "Subject identifier",
    sbp = "Systolic blood pressure (mmHg)"
  )

with_transaction(
  create_table(df_visits, "visits"),
  author = "Data Engineer",
  commit_message = "Add the visit measurements"
)
#> Stored 2 column labels as column comments.
#> Committed snapshot 6 (Data Engineer): Add the visit measurements

df_collected <- get_ducklake_table("visits") |> collect()
var_label(df_collected)
#> $subject
#> [1] "Subject identifier"
#> 
#> $sbp
#> [1] "Systolic blood pressure (mmHg)"
```

The same round trip works without labelled: a label is the `label`
attribute of a column, `attr(df_visits$sbp, "label") <- "..."` sets it,
and `get_table_comments("visits")` shows what was stored. Labels also
follow columns through pipelines. A table derived with
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
from a lake table keeps the comments of the columns it selects, so a
silver or gold layer stays labelled without any extra step.

``` r

detach_ducklake("views_lake")
```
