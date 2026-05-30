# build_quack_uri rejects invalid input

    Code
      ducklake:::build_quack_uri("")
    Condition
      Error in `ducklake:::build_quack_uri()`:
      ! `uri` must be a single, non-empty string.
      i For example "quack:localhost" or "quack:data.example.org:9494".

---

    Code
      ducklake:::build_quack_uri(c("a", "b"))
    Condition
      Error in `ducklake:::build_quack_uri()`:
      ! `uri` must be a single, non-empty string.
      i For example "quack:localhost" or "quack:data.example.org:9494".

---

    Code
      ducklake:::build_quack_uri(NA_character_)
    Condition
      Error in `ducklake:::build_quack_uri()`:
      ! `uri` must be a single, non-empty string.
      i For example "quack:localhost" or "quack:data.example.org:9494".

# attach_quack requires a uri

    Code
      attach_quack("team")
    Condition
      Error in `attach_quack()`:
      ! A `uri` is required.
      i This is the address of the Quack server, for example "quack:localhost".

# quack_query requires a single query string

    Code
      quack_query("quack:localhost", c("a", "b"))
    Condition
      Error in `quack_query()`:
      ! `query` must be a single SQL string.

