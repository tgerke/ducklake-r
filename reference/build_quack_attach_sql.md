# Build the ATTACH SQL for a Quack server

Build the ATTACH SQL for a Quack server

## Usage

``` r
build_quack_attach_sql(
  quack_name,
  uri,
  token = NULL,
  disable_ssl = FALSE,
  ssl_fingerprint = NULL,
  client_id = NULL,
  heartbeat_timeout = NULL
)
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

- ssl_fingerprint, client_id, heartbeat_timeout:

  Optional client options, rendered as `SSL_FINGERPRINT`, `CLIENT_ID`,
  and `HEARTBEAT_TIMEOUT`.

## Value

A SQL ATTACH statement string.
