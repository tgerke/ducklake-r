# Shut down duckplyr's singleton connection and recreate it

Shuts down the singleton DuckDB connection to release file locks, then
recreates it so the session keeps working. The new connection gets the
same macro/R-function setup that duckplyr normally does on first access.

## Usage

``` r
shutdown_and_reset_singleton()
```

## Value

`TRUE` on success, `FALSE` on failure.

## Details

We replace `$con` directly instead of setting it to NULL because
duckplyr stacks `reg.finalizer(onexit = TRUE)` calls that accumulate
across resets. If `$con` is NULL when those finalizers fire at session
exit, each one calls `dbDisconnect(NULL)` and errors. Keeping a valid
connection avoids that.

## Note

This accesses duckplyr internals (`default_duckdb_connection` and
`create_default_duckdb_connection`). Validated against duckplyr 0.4.1.
If duckplyr changes these internals the function returns `FALSE` and the
caller falls back to the warning path.
