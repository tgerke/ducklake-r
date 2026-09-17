# Create a data check from a rule

**Experimental.** Stores a rule as a check: a view, in a schema set
aside for checks, that returns the rows breaking the rule, with the
rule's label as its comment. State the rule the way you would say it, as
the condition every row should meet. `create_check()` keeps the rows
where it is false, so the code reads like the label and a rule such as
"dose is not 0" is written `dose != 0`, with no double negative.
[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
then counts the failing rows of every check in the schema.

## Usage

``` r
create_check(
  .data,
  check_name,
  rule,
  label,
  listing = dplyr::everything(),
  schema_name = NULL,
  replace = TRUE
)
```

## Arguments

- .data:

  A lazy table (a dplyr pipeline built on
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md))
  holding the rows to check. Read the table by its schema-qualified
  name, `get_ducklake_table("main.cars")`, so the check binds whichever
  database is current.

- check_name:

  The rule's id, which becomes the view's name and the `check` column of
  [`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md).

- rule:

  The rule, as an expression on the columns of `.data` that is `TRUE`
  for a row in good standing, such as `cyl %in% c(4, 6, 8)`.

- label:

  One sentence stating the rule, stored as the view's comment.

- listing:

  \<[`tidy-select`](https://dplyr.tidyverse.org/reference/dplyr_tidy_select.html)\>
  The columns the check returns for a failing row: what someone needs to
  find the row and fix it. All columns by default.

- schema_name:

  The schema that holds the checks. Defaults to `"checks"`, or to the
  schema in `check_name` when that is qualified (`"qc.cyl_known"`). It
  is created if it does not exist.

- replace:

  Replace an existing check of the same name (default TRUE).

## Value

Invisibly returns `NULL`.

## Details

A row where the rule evaluates to `NA` passes, as it does under a SQL
`CHECK` constraint: `NOT (rule)` is not true for it. When a missing
value should fail, say so in the rule (`!is.na(dose) & dose != 0`) or
give it a check of its own.

The schema, the view, and its label are written in one transaction, so a
new or revised check is one snapshot. Inside
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
they join the open transaction, which is how a change of rules gets an
author and a commit message.

A check is an ordinary view, and `create_check()` is a convenience for
the common case of one rule about each row. A rule that is easier to
state as its failure (models that appear twice, visits without a
subject) is a pipeline that returns those rows, stored with
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
in the same schema and labelled with
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md).
Both forms select the same rows, since three-valued logic treats
`NOT (dose != 0)` and `dose = 0` alike, so choose the one that reads
better.

## See also

[`run_checks()`](https://tgerke.github.io/ducklake-r/reference/run_checks.md)
to run the checks,
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
for a check written as its failure, and
[`vignette("data-checks")`](https://tgerke.github.io/ducklake-r/articles/data-checks.md).

## Examples

``` r
lake_dir <- tempfile("create_check_lake_")
dir.create(lake_dir)
attach_ducklake("create_check_lake", lake_path = lake_dir)
create_table(
  data.frame(model = rownames(mtcars), mtcars, row.names = NULL),
  "cars"
)

get_ducklake_table("main.cars") |>
  create_check(
    "cyl_known", cyl %in% c(4, 6, 8),
    label = "cyl is 4, 6, or 8",
    listing = c(model, cyl)
  )
#> Created check "cyl_known" in "checks": cyl is 4, 6, or 8

# A rule that forbids a value is stated as it is said
get_ducklake_table("main.cars") |>
  create_check("hp_not_zero", hp != 0, label = "hp is not 0")
#> Created check "hp_not_zero" in "checks": hp is not 0

run_checks()
#>         check             label n_fail
#> 1   cyl_known cyl is 4, 6, or 8      0
#> 2 hp_not_zero       hp is not 0      0

detach_ducklake("create_check_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
