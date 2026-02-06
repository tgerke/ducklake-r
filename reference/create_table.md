# Create a DuckLake table

Create a DuckLake table

## Usage

``` r
create_table(data_source, table_name)
```

## Arguments

- data_source:

  Raw data source. Can be:

  - A URL (http:// or https://)

  - A file path (e.g., "data.csv", "data.parquet")

  - An R data.frame or tibble

  - A lazy table (tbl_duckdb_connection or tbl_lazy)

- table_name:

  Name of the new table

## Examples

``` r
if (FALSE) { # \dontrun{
# From URL
create_table("https://example.com/data.csv", "my_table")

# From local file
create_table("data.csv", "my_table")

# From data.frame
create_table(mtcars, "my_table")

# From lazy table (pipe-friendly)
get_ducklake_table("source_table") %>% 
  filter(x > 5) %>%
  create_table("filtered_table")
} # }
```
