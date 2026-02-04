# Create a DuckLake table

Create a DuckLake table

## Usage

``` r
create_table(table_name, data_source)
```

## Arguments

- table_name:

  Name of the new table

- data_source:

  Raw data source. Can be:

  - A URL (http:// or https://)

  - A file path (e.g., "data.csv", "data.parquet")

  - An R data.frame or tibble

## Examples

``` r
if (FALSE) { # \dontrun{
# From URL
create_table("my_table", "https://example.com/data.csv")

# From local file
create_table("my_table", "data.csv")

# From data.frame
create_table("my_table", mtcars)
} # }
```
