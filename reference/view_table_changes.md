# View a table's change feed interactively

Opens the change feed from
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
in an interactive viewer. A sidebar lists each part of the diff with a
count: the schema changes, the columns and cells that changed, the rows
inserted, updated, and deleted, and the snapshots in range. The main
panel shows the selected part as a table. An update is one row, shown
from its old or its new side, with the changed cells highlighted and
both values on hover; a cells view lists every change as snapshot,
rowid, column, old, new.

## Usage

``` r
view_table_changes(
  changes,
  max_rows = 10000,
  table_name = NULL,
  ducklake_name = NULL,
  conn = NULL,
  width = NULL,
  height = NULL,
  elementId = NULL
)
```

## Arguments

- changes:

  A change feed from
  [`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md):
  the lazy table it returns, possibly narrowed with dplyr verbs (the
  filters run in DuckDB before anything is collected), or that table
  collected into a data frame. The columns `snapshot_id`, `rowid`, and
  `change_type` must still be present.

- max_rows:

  The most feed rows to collect. A longer feed is cut at a snapshot and
  row boundary, never between the two images of one update, and the
  viewer says how many rows it holds out of how many there are. A dplyr
  filter on the feed narrows it.

- table_name:

  The table the feed describes, as `"table"` or `"schema.table"`. A lazy
  feed carries it; a collected data frame does not, and without it the
  viewer shows no snapshot authors and messages and no schema panel.

- ducklake_name:

  Optional name of the attached DuckLake catalog. A lazy feed carries
  it; otherwise the current database is used.

- conn:

  Optional DuckDB connection object. A lazy feed carries it; otherwise
  the default ducklake connection is used.

- width, height:

  The widget's size, in CSS units or pixels. `NULL` fills the viewer.

- elementId:

  An id for the widget's HTML element.

## Value

An htmlwidget of class `ducklake_changes`, which prints in the RStudio
Viewer, a browser, or an HTML document.

## Details

Requires the htmlwidgets package (listed in Suggests).

The layout follows Hadley Wickham's
[data-diff](https://github.com/hadley/data-diff), a command-line tool
with a browser interface for comparing Parquet files, with thanks.
DuckLake's change feed makes the job simpler than comparing two files:
every row carries its identity (`rowid`) and the kind of change, so the
viewer pairs an update's two images by snapshot and rowid and compares
them cell by cell, with no key guessing.

A dplyr filter on a data column can keep one image of an update and drop
the other: `filter(status == "closed")` keeps the image in which the
status is closed. Such a row is shown from the side that is present,
marked as one-sided, with no changed cells. Filters on `rowid`,
`snapshot_id`, or `change_type` never split a pair. DuckLake records an
update even when the new values equal the old, and that shows as an
update with no changed cells.

The schema panel reads the column history from the lake's catalog for
the table's current id, so a rebuild by
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
or
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
inside the range appears as the table's creation. Values are compared
before they are formatted for display: numbers show 15 significant
digits, timestamps are in UTC, and a missing value shows as `NA`. `NA`
and `NaN` compare equal.

## See also

[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
lake_dir <- tempfile("view_lake_")
dir.create(lake_dir)
attach_ducklake("view_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
rows_update(
  get_ducklake_table("orders"),
  data.frame(id = 2L, amount = 25),
  by = "id"
)
rows_delete(get_ducklake_table("orders"), data.frame(id = 3L), by = "id")

# The table's whole history
view_table_changes(get_table_changes("orders"))

{"x":{"title":"orders","table_name":"main.orders","ducklake_name":"view_lake","range":{"bound_type":"snapshot","start":"1","end":"3","start_id":1,"end_id":3},"data":{"id":["1","2","3","2","2","3"],"amount":["10","20","30","25","20","30"]},"columns":{"name":["id","amount"],"type":["integer","number"],"changed_cells":[0,1]},"items":{"kind":["insert","insert","insert","update","delete"],"snapshot_id":[1,1,1,2,3],"rowid":[0,1,2,1,2],"old":[null,null,null,5,6],"new":[1,2,3,4,null],"partial":[false,false,false,false,false],"n_changed":[null,null,null,1,null]},"cells":{"item":[4],"column":["amount"],"old":["20"],"new":["25"]},"snapshots":{"snapshot_id":[1,2,3],"time":["2026-09-15 23:51:50","2026-09-15 23:51:50","2026-09-15 23:51:50"],"author":[null,null,null],"commit_message":[null,null,null],"inserted":[3,0,0],"updated":[0,1,0],"deleted":[0,0,1],"schema_events":[0,0,0]},"schema":{"available":true,"reason":null,"events":{"snapshot_id":[1,1],"kind":["created","created"],"column_id":[1,2],"column_name":["id","amount"],"detail":["int32","float64"],"old_name":[null,null],"new_name":["id","amount"],"old_type":[null,null],"new_type":["int32","float64"]},"columns":{"name":["id","amount"],"type":["int32","float64"],"status":["created","created"]}},"counts":{"inserted":3,"updated":1,"deleted":1,"partial":0,"cells":1,"columns_changed":1,"schema_events":0,"rows_shown":6,"rows_total":6},"truncated":false,"max_rows":10000},"evals":[],"jsHooks":[]}
# Narrow the feed first; the filter runs in DuckDB
get_table_changes("orders") |>
  dplyr::filter(change_type != "insert") |>
  view_table_changes()

{"x":{"title":"orders","table_name":"main.orders","ducklake_name":"view_lake","range":{"bound_type":"snapshot","start":"1","end":"3","start_id":1,"end_id":3},"data":{"id":["2","2","3"],"amount":["25","20","30"]},"columns":{"name":["id","amount"],"type":["integer","number"],"changed_cells":[0,1]},"items":{"kind":["update","delete"],"snapshot_id":[2,3],"rowid":[1,2],"old":[2,3],"new":[1,null],"partial":[false,false],"n_changed":[1,null]},"cells":{"item":[1],"column":["amount"],"old":["20"],"new":["25"]},"snapshots":{"snapshot_id":[1,2,3],"time":["2026-09-15 23:51:50","2026-09-15 23:51:50","2026-09-15 23:51:50"],"author":[null,null,null],"commit_message":[null,null,null],"inserted":[0,0,0],"updated":[0,1,0],"deleted":[0,0,1],"schema_events":[0,0,0]},"schema":{"available":true,"reason":null,"events":{"snapshot_id":[1,1],"kind":["created","created"],"column_id":[1,2],"column_name":["id","amount"],"detail":["int32","float64"],"old_name":[null,null],"new_name":["id","amount"],"old_type":[null,null],"new_type":["int32","float64"]},"columns":{"name":["id","amount"],"type":["int32","float64"],"status":["created","created"]}},"counts":{"inserted":0,"updated":1,"deleted":1,"partial":0,"cells":1,"columns_changed":1,"schema_events":0,"rows_shown":3,"rows_total":3},"truncated":false,"max_rows":10000},"evals":[],"jsHooks":[]}
detach_ducklake("view_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
