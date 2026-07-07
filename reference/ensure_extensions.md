# Install and load required DuckDB extensions for a given backend

Install and load required DuckDB extensions for a given backend

## Usage

``` r
ensure_extensions(backend, encrypted = FALSE)
```

## Arguments

- backend:

  Catalog backend type

- encrypted:

  Whether the lake uses encrypted storage. Writing encrypted files
  requires the full crypto module from the httpfs extension on platforms
  where the built-in module is read-only (notably Windows).
