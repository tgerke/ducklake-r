# Modifying Tables

``` r
library(ducklake)
library(dplyr)

# Setup for examples
install_ducklake()
attach_ducklake("my_ducklake")
create_table("nl_train_stations", "https://blobs.duckdb.org/nl_stations.csv")
```

## Two approaches for table modifications

ducklake provides two complementary approaches for modifying tables,
both following tidyverse conventions:

### 1. data.frame approach (`rows_*` functions)

Best when you have **data in R** (data.frames/tibbles) that you want to
apply to a table:

``` r
# Prepare your data in R
updates <- data.frame(id = c(1, 2), value = c("new1", "new2"))

# Apply to table
rows_update(get_ducklake_table("my_table"), updates, by = "id")  # Update existing rows
rows_insert(get_ducklake_table("my_table"), new_data, by = "id")  # Insert new rows
rows_upsert(get_ducklake_table("my_table"), updates, by = "id")  # Update existing or insert new
rows_delete(get_ducklake_table("my_table"), to_delete, by = "id")  # Delete rows by key
```

**Pros:** Explicit, familiar dplyr syntax, `in_place = TRUE` by default
for DuckLake  
**Use when:** You have data.frames/tibbles ready to apply

### 2. Pipeline approach (`*_table` functions)

Best when you’re **transforming data with dplyr** and want to apply
results to a table:

``` r
# Build transformation pipeline, then execute
get_ducklake_table("my_table") |>
  filter(status == "active") |>
  mutate(processed = TRUE) |>
  ducklake_exec()  # for updates

source_table |>
  select(id, value) |>
  mutate(value = toupper(value)) |>
  upsert_table("target_table", by = "id")  # for merge/upsert
```

**Pros:** Chainable, works in pipelines, table name inference  
**Use when:** Transforming data with
[`filter()`](https://dplyr.tidyverse.org/reference/filter.html),
[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html),
[`summarize()`](https://dplyr.tidyverse.org/reference/summarise.html),
etc.

## Examples

### Update with `rows_update()`

``` r
# update the first row with ducklake::rows_update()
# copy = TRUE and in_place = TRUE are the defaults for DuckLake operations
rows_update(
  get_ducklake_table("nl_train_stations"),
  data.frame(
    uic = 8400319,
    name_short = "NEW"
  ),
  by = "uic",
  copy = TRUE,
  unmatched = "ignore"
)

# View the change
get_ducklake_table("nl_train_stations") |>
  filter(uic == 8400319) |>
  select(uic, name_short)
```

### Update with pipeline and `ducklake_exec()`

``` r
# update with mutate and ducklake::ducklake_exec
# table name is automatically inferred from the pipeline
get_ducklake_table("nl_train_stations") |>
  mutate(
    name_long = dplyr::case_when(
      code == "ASB" ~ "Johan Cruijff ArenA",
      .default = name_long
    )
  ) |>
  ducklake_exec()
```

### Preview SQL before execution

``` r
# if we want, we can always view the sql that will be submitted in advance
get_ducklake_table("nl_train_stations") |>
  mutate(
    name_long = dplyr::case_when(
      code == "ASB" ~ "Johan Cruijff ArenA",
      .default = name_long
    )
  ) |>
  show_ducklake_query()
```

### Filter and execute

``` r
# filter using ducklake::ducklake_exec
# with .quiet=FALSE we can see sql on execution, including the original dplyr
get_ducklake_table("nl_train_stations") |>
  filter(uic == 8400319 | code == "ASB") |>
  ducklake_exec(.quiet = FALSE)

# show our current table
get_ducklake_table("nl_train_stations")
```
