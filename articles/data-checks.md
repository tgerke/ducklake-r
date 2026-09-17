# Data Checks

``` r

library(ducklake)
library(dplyr)

attach_ducklake("checks_lake", lake_path = vignette_temp_dir)

df_cars <- data.frame(model = rownames(mtcars), mtcars, row.names = NULL)

with_transaction(
  create_table(df_cars, "cars"),
  author = "Data Engineer",
  commit_message = "Add the Motor Trend car data"
)
```

DuckLake enforces one rule about the values in a table: a column can be
`NOT NULL`. It has no check constraints, no primary or unique keys, and
no foreign keys, and its catalog accepts no custom tags from SQL, so a
rule like “`cyl` is 4, 6, or 8” has no slot of its own. The catalog does
hold views. A view that returns the rows breaking a rule is a check: the
rule’s logic sits in the catalog, it is versioned with the data it
describes, and any client of the lake runs it by reading the view.

This article writes checks that way and runs them with
[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md).
Then it uses what a lake adds: a load that rolls back when a check
fails, the outcome of the checks recorded on a commit, and the rules and
the data read together as of an earlier snapshot.
[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
is experimental while the convention settles.

## The rule the lake enforces

[`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md)
sets the one constraint DuckLake has. From then on the lake itself
refuses a row without the value, whichever client writes it. Plain SQL
stands in for another client here:

``` r

with_transaction(
  set_column_not_null("cars", "mpg"),
  author = "Data Manager",
  commit_message = "Require mpg"
)
#> Column "mpg" in "cars" now requires a value.
#> Committed snapshot 2 (Data Manager): Require mpg

try(
  DBI::dbExecute(
    get_ducklake_connection(),
    "INSERT INTO cars (model, cyl) VALUES ('Unknown', 4)"
  )
)
#> Error in duckdb_result(connection = conn, stmt_lst = stmt_lst, arrow = arrow) : 
#>   Invalid Error: Constraint Error: NOT NULL constraint failed: cars.mpg
#> ℹ Context: rapi_execute
#> ℹ Error type: INVALID
```

## Rules as views

Every other rule is a view. The checks get a schema of their own (a
schema is the lake’s namespace for tables and views), which keeps them
apart from the data and tells
[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
where to look. Each one is a dplyr pipeline that keeps the rows breaking
the rule, stored with
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
and labelled with
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md):

``` r

with_transaction({
  create_schema("checks")

  get_ducklake_table("main.cars") |>
    filter(!(cyl %in% c(4, 6, 8))) |>
    select(model, cyl) |>
    create_view("checks.cyl_known")
  set_table_comment("checks.cyl_known", "cyl is 4, 6, or 8")

  get_ducklake_table("main.cars") |>
    filter(!(hp > 0 & hp < 500)) |>
    select(model, hp) |>
    create_view("checks.hp_plausible")
  set_table_comment("checks.hp_plausible", "hp is between 0 and 500")
}, author = "Data Manager", commit_message = "Add the first checks on cars")
#> Created schema "checks".
#> Created view "checks.cyl_known".
#> Commented view "checks.cyl_known".
#> Created view "checks.hp_plausible".
#> Commented view "checks.hp_plausible".
#> Committed snapshot 3 (Data Manager): Add the first checks on cars
```

Four habits keep a check dependable:

- State the rule and negate it: `filter(!(rule))`. In SQL, `NOT` is not
  true for a row where the rule evaluates to `NA`, so a missing `cyl`
  passes `cyl_known`. When a missing value matters, give it a check of
  its own, or make the column `NOT NULL`.
- Select the columns a reviewer needs. The view is the listing of what
  failed, so its columns are the ones someone will act on.
- Read the table by its schema-qualified name, `"main.cars"`. DuckDB
  resolves an unqualified name from the view’s schema and the session’s
  current database, so `"cars"` can fail to bind for a client where
  another database is current.
- Keep only checks in the schema.
  [`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
  counts the rows of every view it finds there, and a view that
  summarizes (a row of totals) would report a failure every time.

A rule that spans tables is a join. An
[`anti_join()`](https://dplyr.tidyverse.org/reference/filter-joins.html)
of visits against subjects, for example, returns the visits with no
subject.

## Running the checks

[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
counts the rows each check returns:

``` r

run_checks()
#>          check                   label n_fail
#> 1    cyl_known       cyl is 4, 6, or 8      0
#> 2 hp_plausible hp is between 0 and 500      0
```

The label comes from the view’s comment and `n_fail` is the number of
rows breaking the rule. The car data passes both.

## Gating a load

A new batch arrives, and one of its rows has a typing error:

``` r

df_batch <- data.frame(
  model = c("Saab 99", "Audi 100 LS"),
  mpg = c(24.5, 23.0),
  cyl = c(4, 40),
  hp = c(87, 91)
)
```

Inside a transaction the checks see the rows that are waiting to be
committed. So a load can run the checks on itself and stop, and
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
rolls everything back when it does:

``` r

try(
  with_transaction({
    rows_insert(get_ducklake_table("cars"), df_batch, by = "model")

    df_checks <- run_checks()
    if (any(df_checks$n_fail > 0)) {
      stop("failed ", toString(df_checks$check[df_checks$n_fail > 0]))
    }
  }, author = "Data Manager", commit_message = "Load the September batch")
)
#> Transaction rolled back.
#> Error : Transaction rolled back due to error: failed cyl_known
```

Nothing of the batch reached the lake. The table has its 32 rows and the
history has no new snapshot:

``` r

get_ducklake_table("cars") |> count() |> collect()
#> # A tibble: 1 × 1
#>       n
#>   <dbl>
#> 1    32

list_table_snapshots("cars") |>
  select(snapshot_id, author, commit_message)
#>   snapshot_id        author               commit_message
#> 1           1 Data Engineer Add the Motor Trend car data
#> 2           2  Data Manager                  Require mpg
```

This is how a rule becomes a constraint in a lake. DuckLake would accept
the row; a transaction that checks itself never commits it. The gate
covers the writers that use it, and a client that writes without running
the checks is not stopped.

## Recording the outcome on the commit

Rejecting the batch is not always right. Raw data often has to land as
it arrived, with the problems flagged for follow-up. A manual
transaction can run the checks after the write and put the outcome on
the commit, in `commit_extra_info`:

``` r

begin_transaction()
rows_insert(get_ducklake_table("cars"), df_batch, by = "model")

df_failed <- run_checks() |>
  filter(n_fail > 0) |>
  select(check, n_fail)

commit_transaction(
  author = "Data Manager",
  commit_message = "Load the September batch",
  commit_extra_info = as.character(
    jsonlite::toJSON(list(checks_failed = df_failed))
  )
)
#> Committed snapshot 4 (Data Manager): Load the September batch

v_loaded <- max(list_table_snapshots("cars")$snapshot_id)
```

The snapshot now says what state the data was in when it was committed:

``` r

list_table_snapshots("cars") |>
  select(snapshot_id, commit_message, commit_extra_info) |>
  tail(1)
#>   snapshot_id           commit_message
#> 3           4 Load the September batch
#>                                      commit_extra_info
#> 3 {"checks_failed":[{"check":"cyl_known","n_fail":1}]}
```

## Which rows failed

The check is a view, so the failing rows are one read away, with the
columns the check selected:

``` r

run_checks()
#>          check                   label n_fail
#> 1    cyl_known       cyl is 4, 6, or 8      1
#> 2 hp_plausible hp is between 0 and 500      0

get_ducklake_table("checks.cyl_known") |> collect()
#> # A tibble: 1 × 2
#>   model         cyl
#>   <chr>       <dbl>
#> 1 Audi 100 LS    40
```

The correction is an ordinary update, and its commit message can name
the check it answers. Afterwards the lake’s history holds the load, the
finding, and the fix:

``` r

with_transaction(
  rows_update(
    get_ducklake_table("cars"),
    data.frame(model = "Audi 100 LS", cyl = 4),
    by = "model"
  ),
  author = "Data Manager",
  commit_message = "cyl_known: Audi 100 LS cyl corrected from 40 to 4"
)
#> Committed snapshot 5 (Data Manager): cyl_known: Audi 100 LS cyl corrected from
#> 40 to 4

run_checks()
#>          check                   label n_fail
#> 1    cyl_known       cyl is 4, 6, or 8      0
#> 2 hp_plausible hp is between 0 and 500      0
```

## Rules have history

A check is a catalog object, so changing a rule is a snapshot with an
author and a message, like a change to the data.
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
replaces a view of the same name and keeps its label. This revision
changes what the rule says, so it sets a new label in the same
transaction:

``` r

with_transaction({
  get_ducklake_table("main.cars") |>
    filter(!(hp >= 40 & hp <= 400)) |>
    select(model, hp) |>
    create_view("checks.hp_plausible")
  set_table_comment("checks.hp_plausible", "hp is between 40 and 400")
}, author = "Data Manager", commit_message = "Narrow the plausible hp range")
#> Created view "checks.hp_plausible".
#> Commented view "checks.hp_plausible".
#> Committed snapshot 6 (Data Manager): Narrow the plausible hp range

list_table_snapshots("checks.hp_plausible") |>
  select(snapshot_id, author, commit_message)
#>   snapshot_id       author                commit_message
#> 1           3 Data Manager  Add the first checks on cars
#> 2           6 Data Manager Narrow the plausible hp range
```

## Rules and data as of a snapshot

A lake attached at a snapshot shows its views as they were then, along
with its tables. Re-attaching at the snapshot of the September load runs
the rules of that day against the data of that day. The uncorrected row
fails again, and the `hp` rule reads as it did then:

``` r

detach_ducklake("checks_lake")
attach_ducklake(
  "checks_lake",
  lake_path = vignette_temp_dir,
  snapshot_version = v_loaded
)

run_checks()
#>          check                   label n_fail
#> 1    cyl_known       cyl is 4, 6, or 8      1
#> 2 hp_plausible hp is between 0 and 500      0
```

That answers a question an audit asks: which rules were in force when
this data was committed, and did the data pass them?

## Where other tools fit

[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
counts and nothing more. A report is a job for the tools built for one:
the rows collected from a check view are a data frame, ready for gt, or
for a validation report from affirm or pointblank. pointblank also
interrogates lazy tables, so one of its agents can run on
`get_ducklake_table("cars")` inside DuckDB. For a dictionary of a lake’s
tables and columns that travels as a file,
[data-dict](https://data-dict.tidyverse.org) defines a YAML format.
Comments and labels, the descriptive half of a dictionary, are covered
in
[`vignette("views-comments-labels")`](https://tgerke.github.io/ducklake-r/articles/views-comments-labels.md).

``` r

detach_ducklake("checks_lake")
```
