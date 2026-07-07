# Normalize a Quack URI

Validates that `uri` is a single non-empty string and prepends the
`quack:` scheme if it is missing.

## Usage

``` r
build_quack_uri(uri)
```

## Arguments

- uri:

  A Quack server address.

## Value

The normalized URI string.
