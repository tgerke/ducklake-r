# Build the ATTACH SQL for a Quack server

Build the ATTACH SQL for a Quack server

## Usage

``` r
build_quack_attach_sql(quack_name, uri, token = NULL, disable_ssl = FALSE)
```

## Arguments

- quack_name:

  Name for the remote catalog alias.

- uri:

  Quack server address.

- token:

  Optional authentication token.

- disable_ssl:

  Whether to add the `DISABLE_SSL` option.

## Value

A SQL ATTACH statement string.
