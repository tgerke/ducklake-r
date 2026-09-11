# Getting Started with ducklake

``` r

library(ducklake)
library(dplyr)
```

## What a DuckLake is

A DuckLake is a data lake that keeps its own history. Two pieces make it
up: a catalog database that records every table, column, and change, and
a directory of Parquet files that holds the rows. Every committed change
becomes a snapshot, and any snapshot can be read again later, so a
table’s past is part of the table. ducklake puts dplyr on top of this.
You attach a lake, add tables, and query them as lazy tables, and DuckDB
does the work.

This article follows one short session: attach a lake, add a table, read
it back, change it, look at its history, and detach. [Choosing a
Deployment](https://tgerke.github.io/ducklake-r/articles/deployment.md)
describes what a lake looks like on disk and where the catalog and the
data files can live.

## Install

``` r

install.packages("ducklake")
```

ducklake needs the duckdb package at version 1.5.5 or newer. DuckLake
itself is a DuckDB extension, and the first
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
downloads it and says where it went. The first time duckdb connects in
an interactive session it offers to create `~/.duckdb`; say yes, and the
download happens once instead of once per session. For scripts and CI,
set `DUCKDB_R_HOME` in `~/.Renviron` or the job’s environment.

## Attach a lake

One call opens a lake, and creates it first when nothing is at the path
yet:

``` r

attach_ducklake("my_lake", lake_path = vignette_temp_dir, author = "Data Engineer")
```

The same call with the same path opens the lake again in a later
session. When you know a lake already exists, add `create = FALSE`: a
mistyped path then stops with an error instead of creating a new, empty
lake.

By default the catalog is a DuckDB database file stored next to the
data, which is the right setup for one person on one machine. A lake
that several people write to keeps its catalog in SQLite or PostgreSQL
instead, and that is a one-argument change to this call. [Choosing a
Deployment](https://tgerke.github.io/ducklake-r/articles/deployment.md)
walks through the options.

## Add a table

[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
takes a data frame and writes it into the lake as a new table. Wrapped
in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md),
the table is committed as one snapshot with a record of who added it and
why:

``` r

with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Add the Motor Trend car data"
)
#> Committed snapshot 1 (Data Engineer): Add the Motor Trend car data

list_ducklake_tables()
#>   schema_name table_name  type
#> 1        main       cars table
```

The confirmation names the snapshot the commit created. That number is
the table’s version, and the history section below uses it to look back.

### Why most calls sit inside `with_transaction()`

Every change to a lake is a commit. Called on its own,
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
still commits, as one snapshot without an author or a message.
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
makes the commit explicit. It groups everything inside it into a single
snapshot, records who made the change and why, and rolls everything back
if any step fails, so a half-finished change never lands in the lake.

The name follows the [withr](https://withr.r-lib.org/) convention: a
`with_*()` function runs the code you hand it inside a temporary state
and cleans up afterward. Here the state is an open transaction, and the
cleanup is a commit on success or a rollback on error.

The first argument is one R expression. A single call needs nothing
more. To commit several operations as one snapshot, wrap them in braces,
exactly as you would write the body of a function. The change below does
that.

Three fields describe a commit. `author` and `commit_message` are the
ones to use every time. `commit_extra_info` holds free-form context,
such as a ticket number or a JSON string, and it appears alongside the
other two in the snapshot history. [Working with
Transactions](https://tgerke.github.io/ducklake-r/articles/transactions.md)
covers all three, manual transaction control, and what happens when
several sessions write at once.

## Read a table

[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
returns a lazy table. Nothing is read until you ask for it, and the
dplyr verbs you pipe onto it become SQL that DuckDB runs against the
lake:

``` r

cars_data <- get_ducklake_table("cars")

cars_data |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  head(3)
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmprFd65L/ducklake/ducklake2b024c526014.duckdb]
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  21       6   110
#> 2  21       6   110
#> 3  21.4     6   110
```

[`show_query()`](https://dplyr.tidyverse.org/reference/explain.html)
prints the SQL a pipeline will run.
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html) runs
it and returns a tibble:

``` r

cars_data |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  show_query()
#> <SQL>
#> SELECT mpg, cyl, hp
#> FROM cars
#> WHERE (cyl = 6.0)

df_six <- cars_data |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  collect()

df_six
#> # A tibble: 7 × 3
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  21       6   110
#> 2  21       6   110
#> 3  21.4     6   110
#> 4  18.1     6   105
#> 5  19.2     6   123
#> 6  17.8     6   123
#> 7  19.7     6   175
```

Filter and select before you collect. DuckDB then reads only the rows
and columns you need, which matters once a table is larger than memory.

## Change a table

A dplyr pipeline on a lake table is a query. On its own,
[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) computes
a column in the result you would collect and changes nothing in the
lake. To change the stored rows, end the pipeline with something that
writes.
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
in the section above is one such function, which is why it needed
nothing more. Which one to end with depends on what you want:

| You want to | End the pipeline with |
|----|----|
| Bring rows into R | [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) |
| Make a new table from the result | [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md) |
| Change rows of this table in place | [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md) |
| Rewrite this table from the result | [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md) |

[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
turns [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
into the new values and
[`filter()`](https://dplyr.tidyverse.org/reference/filter.html) into
which rows receive them.
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
is a separate matter: it decides how a write is recorded, not whether it
happens.

Here two steps make one snapshot: a new column is declared, then filled
in place through
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md).
The braces commit both steps as a unit:

``` r

with_transaction({
  add_table_column("cars", "kpl", "DOUBLE")
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>
    ducklake_exec()
},
  author = "Data Engineer",
  commit_message = "Add fuel efficiency in km/L"
)
#> Added column "kpl" (DOUBLE) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
#> Committed snapshot 2 (Data Engineer): Add fuel efficiency in km/L

get_ducklake_table("cars") |>
  select(mpg, kpl) |>
  head(3)
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmprFd65L/ducklake/ducklake2b024c526014.duckdb]
#>     mpg   kpl
#>   <dbl> <dbl>
#> 1  21    8.93
#> 2  21    8.93
#> 3  22.8  9.69
```

[Modifying
Tables](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md)
covers the other ways to change a table:
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
and
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
for specific rows,
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
and
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
for batches, and
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for a bulk rewrite. All of them are versioned in the same way.

## See the history

[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
lists every snapshot that touched a table:

``` r

list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-11 15:06:01              1
#> 2           2 2026-09-11 15:06:01              2
#>                                                              changes
#> 1                 tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_altered, tables_inserted_into, tables_deleted_from, 1, 1, 1
#>          author               commit_message commit_extra_info
#> 1 Data Engineer Add the Motor Trend car data              <NA>
#> 2 Data Engineer  Add fuel efficiency in km/L              <NA>
```

Each row is one commit. `changes` summarizes what the commit did,
`author`, `commit_message`, and `commit_extra_info` are the fields
passed to
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md),
or to
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
for snapshot 0, the lake’s creation, and `snapshot_id` is the number
from the confirmation. Any earlier version can be read as a lazy table:

``` r

# The cars table before the kpl column existed
get_ducklake_table_version("cars", version = 1) |>
  head(3)
#> # A query:  ?? x 11
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmprFd65L/ducklake/ducklake2b024c526014.duckdb]
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4
#> 3  22.8     4   108    93  3.85  2.32  18.6     1     1     4     1
```

A table can also be put back the way it was. The restore is a new
snapshot, so the versions after it stay readable:

``` r

restore_table_version("cars", version = 1, author = "Data Engineer")
#> Committed snapshot 3 (Data Engineer): Restored cars to snapshot 1

list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-11 15:06:01              1
#> 2           2 2026-09-11 15:06:01              2
#> 3           3 2026-09-11 15:06:02              3
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2    tables_altered, tables_inserted_into, tables_deleted_from, 1, 1, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#>          author               commit_message commit_extra_info
#> 1 Data Engineer Add the Motor Trend car data              <NA>
#> 2 Data Engineer  Add fuel efficiency in km/L              <NA>
#> 3 Data Engineer  Restored cars to snapshot 1              <NA>
```

[Time
Travel](https://tgerke.github.io/ducklake-r/articles/time-travel.md)
covers reading a table as of a timestamp, comparing versions, and
pinning a whole session to one snapshot.

## Detach

``` r

detach_ducklake("my_lake")
```

Detaching deletes nothing. It matters because DuckDB holds a lock on the
catalog file while the lake is attached, so another R session, a backup,
or a restore cannot open the lake until this one lets go.
`detach_ducklake("my_lake", shutdown = TRUE)` also closes the DuckDB
connection. Attaching the same path again picks up where you left off,
history included.

## Where to go next

- [Loading
  Data](https://tgerke.github.io/ducklake-r/articles/loading-data.md):
  files, URLs, pipelines, schemas, Parquet files that are already on
  disk, and migrating from DuckDB or Iceberg.
- [Modifying
  Tables](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md):
  choosing between row operations, upserts, merges, and rewrites.
- [Views, Comments, and
  Labels](https://tgerke.github.io/ducklake-r/articles/views-comments-labels.md):
  shared query logic and documentation that live in the lake.
- [Working with
  Transactions](https://tgerke.github.io/ducklake-r/articles/transactions.md):
  commit metadata, manual control, and several writers at once.
- [Time
  Travel](https://tgerke.github.io/ducklake-r/articles/time-travel.md):
  every way to read the past.
- [Choosing a
  Deployment](https://tgerke.github.io/ducklake-r/articles/deployment.md):
  catalog backends, object storage, access, and day-one settings.
- [Storage and
  Backups](https://tgerke.github.io/ducklake-r/articles/storage-and-backups.md):
  the files on disk, backups, and maintenance.
- [Clinical Trial Data
  Lake](https://tgerke.github.io/ducklake-r/articles/clinical-trial-datalake.md):
  a complete regulated workflow.
