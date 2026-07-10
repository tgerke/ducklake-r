# Store object storage credentials for a session

Registers credentials with DuckDB's secrets manager so a DuckLake can
read and write data files on object storage (`lake_path = "s3://..."`
and friends). Wraps `CREATE SECRET`.

## Usage

``` r
create_storage_secret(
  type = c("s3", "gcs", "r2", "azure"),
  ...,
  provider = NULL,
  scope = NULL,
  name = NULL,
  persistent = FALSE
)
```

## Arguments

- type:

  Storage type: `"s3"` (also for S3-compatible stores), `"gcs"` (Google
  Cloud Storage), `"r2"` (Cloudflare R2), or `"azure"`.

- ...:

  Named secret parameters passed through to `CREATE SECRET`, e.g.
  `key_id`, `secret`, `region`, `session_token`, `endpoint`,
  `url_style`, `account_id` (R2), or `connection_string` (Azure).
  Character values are quoted; logicals become `true`/`false`.

- provider:

  Optional credential provider. The common one is `"credential_chain"`,
  which picks up credentials the way AWS SDKs do (environment variables,
  profiles, instance metadata) so no key needs to be passed in code.

- scope:

  Optional URI prefix (e.g. `"s3://my-bucket"`) limiting which paths the
  secret applies to. Useful when different buckets need different
  credentials.

- name:

  Optional name for the secret. Named secrets can be replaced and
  dropped individually; unnamed ones act as the default for their type.

- persistent:

  If `TRUE`, the secret is written (unencrypted) to
  `~/.duckdb/stored_secrets` and survives the session. The default
  `FALSE` keeps it in memory only, which is the right choice for
  credentials supplied from a vault or environment variable.

## Value

Invisibly returns the secret's name (or `NA_character_` for an unnamed
secret).

## Details

The httpfs extension (or the azure extension for `type = "azure"`) is
loaded automatically.

Prefer `provider = "credential_chain"` over embedding long-lived keys in
scripts. The secret's values are visible in the session via
`duckdb_secrets()` (redacted) and travel with persistent storage
unencrypted, so treat `persistent = TRUE` with the same care as a
credentials file.

## See also

[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Explicit keys, scoped to one bucket
create_storage_secret(
  "s3",
  key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  region = "us-east-1",
  scope = "s3://my-trial-lake"
)

# Let the AWS credential chain find credentials
create_storage_secret("s3", provider = "credential_chain")

# Then attach a lake whose data lives on S3
attach_ducklake("trial_lake", lake_path = "s3://my-trial-lake/data")
} # }
```
