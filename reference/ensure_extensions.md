# Install and load required DuckDB extensions for a given backend

Install and load required DuckDB extensions for a given backend

## Usage

``` r
ensure_extensions(backend, encrypted = FALSE, remote = FALSE)
```

## Arguments

- backend:

  Catalog backend type

- encrypted:

  Whether the lake uses encrypted storage. Writing encrypted files
  requires the full crypto module from the httpfs extension on platforms
  where the built-in module is read-only (notably Windows).

- remote:

  Whether the data path or catalog path is a remote URI, in which case
  httpfs handles the IO. Loading it up front beats relying on DuckDB's
  mid-statement autoload, which can fail.

## Value

Invisibly, `NULL`. Called for its side effect of loading (and, if
necessary, installing) the required DuckDB extensions.
