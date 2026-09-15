#' View a table's change feed interactively
#'
#' Opens the change feed from [get_table_changes()] in an interactive
#' viewer. A sidebar lists each part of the diff with a count: the schema
#' changes, the columns and cells that changed, the rows inserted, updated,
#' and deleted, and the snapshots in range. The main panel shows the
#' selected part as a table. An update is one row, shown from its old or its
#' new side, with the changed cells highlighted and both values on hover; a
#' cells view lists every change as snapshot, rowid, column, old, new.
#'
#' @param changes A change feed from [get_table_changes()]: the lazy table
#'   it returns, possibly narrowed with dplyr verbs (the filters run in
#'   DuckDB before anything is collected), or that table collected into a
#'   data frame. The columns `snapshot_id`, `rowid`, and `change_type`
#'   must still be present.
#' @param max_rows The most feed rows to collect. A longer feed is cut at a
#'   snapshot and row boundary, never between the two images of one update,
#'   and the viewer says how many rows it holds out of how many there are.
#'   A dplyr filter on the feed narrows it.
#' @param table_name The table the feed describes, as `"table"` or
#'   `"schema.table"`. A lazy feed carries it; a collected data frame does
#'   not, and without it the viewer shows no snapshot authors and messages
#'   and no schema panel.
#' @param ducklake_name Optional name of the attached DuckLake catalog. A
#'   lazy feed carries it; otherwise the current database is used.
#' @param conn Optional DuckDB connection object. A lazy feed carries it;
#'   otherwise the default ducklake connection is used.
#' @param width,height The widget's size, in CSS units or pixels. `NULL`
#'   fills the viewer.
#' @param elementId An id for the widget's HTML element.
#'
#' @returns An htmlwidget of class `ducklake_changes`, which prints in the
#'   RStudio Viewer, a browser, or an HTML document.
#' @family time travel
#' @export
#'
#' @details
#' Requires the htmlwidgets package (listed in Suggests).
#'
#' The layout follows Hadley Wickham's
#' [data-diff](https://github.com/hadley/data-diff), a command-line tool
#' with a browser interface for comparing Parquet files, with thanks.
#' DuckLake's change feed makes the job simpler than comparing two files:
#' every row carries its identity (`rowid`) and the kind of change, so the
#' viewer pairs an update's two images by snapshot and rowid and compares
#' them cell by cell, with no key guessing.
#'
#' A dplyr filter on a data column can keep one image of an update and drop
#' the other: `filter(status == "closed")` keeps the image in which the
#' status is closed. Such a row is shown from the side that is present,
#' marked as one-sided, with no changed cells. Filters on `rowid`,
#' `snapshot_id`, or `change_type` never split a pair. DuckLake records an
#' update even when the new values equal the old, and that shows as an
#' update with no changed cells.
#'
#' The schema panel reads the column history from the lake's catalog for
#' the table's current id, so a rebuild by [replace_table()] or
#' [restore_table_version()] inside the range appears as the table's
#' creation. Values are compared before they are formatted for display:
#' numbers show 15 significant digits, timestamps are in UTC, and a missing
#' value shows as `NA`. `NA` and `NaN` compare equal.
#'
#' @seealso [get_table_changes()], [plot_table_changes()],
#'   [list_table_snapshots()]
#'
#' @examplesIf ducklake_extension_available() && requireNamespace("htmlwidgets", quietly = TRUE)
#' lake_dir <- tempfile("view_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("view_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#' rows_update(
#'   get_ducklake_table("orders"),
#'   data.frame(id = 2L, amount = 25),
#'   by = "id"
#' )
#' rows_delete(get_ducklake_table("orders"), data.frame(id = 3L), by = "id")
#'
#' # The table's whole history
#' view_table_changes(get_table_changes("orders"))
#'
#' # Narrow the feed first; the filter runs in DuckDB
#' get_table_changes("orders") |>
#'   dplyr::filter(change_type != "insert") |>
#'   view_table_changes()
#'
#' detach_ducklake("view_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
view_table_changes <- function(changes, max_rows = 10000, table_name = NULL,
                               ducklake_name = NULL, conn = NULL,
                               width = NULL, height = NULL, elementId = NULL) {
  if (!requireNamespace("htmlwidgets", quietly = TRUE)) {
    cli::cli_abort(c(
      "{.pkg htmlwidgets} is required for {.fn view_table_changes}.",
      "i" = "Install it with {.code install.packages(\"htmlwidgets\")}."
    ))
  }

  payload <- build_changes_payload(
    changes, max_rows, table_name, ducklake_name, conn
  )

  htmlwidgets::createWidget(
    name = "ducklake_changes",
    x = payload,
    width = width,
    height = height,
    package = "ducklake",
    elementId = elementId,
    sizingPolicy = htmlwidgets::sizingPolicy(
      defaultWidth = "100%",
      defaultHeight = 600,
      padding = 0,
      viewer.fill = TRUE,
      viewer.padding = 0,
      browser.fill = TRUE,
      browser.padding = 0,
      # Not a figure: the vignettes' fig.height would squash the widget
      knitr.figure = FALSE,
      knitr.defaultWidth = "100%",
      knitr.defaultHeight = 520
    )
  )
}

#' Everything the change viewer draws, computed in R
#'
#' Collects and caps the feed, pairs the two images of each update by
#' snapshot and rowid, finds the cells that changed, counts by snapshot and
#' by column, and reads the column history from the catalog. Returns a plain
#' list that serializes to the widget's JSON; the JavaScript only draws it.
#' Data frames in the list serialize column-major, so the JavaScript reads
#' `items.kind[i]`; `items$old` and `items$new` are 1-based positions in the
#' collected feed.
#'
#' @param changes A lazy or collected change feed.
#' @param max_rows Cap on collected rows; `Inf` for no cap.
#' @param table_name,ducklake_name,conn Overrides for what a lazy feed
#'   carries in its `ducklake_changes` attribute.
#' @returns A list.
#' @noRd
build_changes_payload <- function(changes, max_rows = 10000, table_name = NULL,
                                  ducklake_name = NULL, conn = NULL) {
  if (!is.numeric(max_rows) || length(max_rows) != 1 || is.na(max_rows) ||
      max_rows < 2) {
    cli::cli_abort("{.arg max_rows} must be a single number of at least 2.")
  }
  if (is.finite(max_rows)) {
    max_rows <- as.integer(min(floor(max_rows), .Machine$integer.max))
  }
  info <- attr(changes, "ducklake_changes")

  if (inherits(changes, "tbl_lazy")) {
    check_feed_columns(colnames(changes))
    if (is.null(conn)) {
      conn <- dbplyr::remote_con(changes)
    }
    collected <- collect_change_feed(changes, max_rows)
  } else if (is.data.frame(changes)) {
    check_feed_columns(names(changes))
    ordered <- changes[
      order(changes$snapshot_id, changes$rowid, changes$change_type), ,
      drop = FALSE
    ]
    collected <- cut_change_feed(ordered, max_rows, total = nrow(changes))
  } else {
    cli::cli_abort(c(
      "{.arg changes} must be a change feed from {.fn get_table_changes}.",
      "i" = "Pass the lazy table it returns, or that table collected into a data frame."
    ))
  }
  rows <- drop_unknown_changes(collected$rows)

  # An explicit table name wins over what the feed carries
  if (!is.null(table_name)) {
    ref <- resolve_table_ref(table_name)
    schema <- if (is.null(ref$schema)) "main" else ref$schema
    table <- ref$table
  } else if (!is.null(info)) {
    schema <- info$schema
    table <- info$table
  } else {
    schema <- NULL
    table <- NULL
  }
  has_table <- !is.null(table)
  if (has_table) {
    if (is.null(conn)) {
      conn <- get_ducklake_connection()
    }
    if (is.null(ducklake_name)) {
      ducklake_name <- if (!is.null(info)) {
        info$ducklake_name
      } else {
        infer_ducklake_name(NULL, conn)
      }
    }
  }

  feed_cols <- c("snapshot_id", "rowid", "change_type")
  data_cols <- setdiff(names(rows), feed_cols)
  data <- lapply(rows[data_cols], function(x) I(format_cell(x)))
  names(data) <- data_cols
  columns <- data.frame(
    name = data_cols,
    type = vapply(rows[data_cols], simple_type_label, character(1)),
    stringsAsFactors = FALSE
  )

  items <- pair_change_rows(rows)
  cells <- changed_cells(rows, items, data, data_cols)
  is_pair <- items$kind == "update" & !items$partial
  items$n_changed <- rep(NA_integer_, nrow(items))
  items$n_changed[is_pair] <- as.integer(
    table(factor(cells$item, levels = seq_len(nrow(items))))
  )[is_pair]
  columns$changed_cells <- as.integer(
    table(factor(cells$column, levels = data_cols))
  )

  snapshot_info <- feed_snapshots(rows, items, info, has_table, table, schema,
                                  ducklake_name, conn)
  snapshots <- snapshot_info$snapshots
  start_id <- snapshot_info$start_id
  end_id <- snapshot_info$end_id

  schema_panel <- if (has_table && !is.na(start_id)) {
    schema_history(table, schema, ducklake_name, conn, start_id, end_id)
  } else {
    list(
      available = FALSE,
      reason = if (has_table) "the feed is empty" else "the feed does not name its table",
      events = empty_schema_events(),
      columns = empty_schema_columns()
    )
  }
  is_change <- schema_panel$events$kind != "created"
  snapshots$schema_events <- as.integer(table(factor(
    schema_panel$events$snapshot_id[is_change], levels = snapshots$snapshot_id
  )))

  list(
    title = if (!has_table) {
      "Table changes"
    } else if (schema == "main") {
      table
    } else {
      paste0(schema, ".", table)
    },
    table_name = if (has_table) paste0(schema, ".", table) else NA_character_,
    ducklake_name = if (has_table) ducklake_name else NA_character_,
    range = list(
      bound_type = if (is.null(info)) NA_character_ else info$bound_type,
      start = if (is.null(info)) NA_character_ else format_bound(info$start),
      end = if (is.null(info)) NA_character_ else format_bound(info$end),
      start_id = start_id,
      end_id = end_id
    ),
    data = data,
    columns = columns,
    items = items,
    cells = cells,
    snapshots = snapshots,
    schema = schema_panel,
    counts = list(
      inserted = sum(items$kind == "insert"),
      updated = sum(items$kind == "update"),
      deleted = sum(items$kind == "delete"),
      partial = sum(items$partial),
      cells = nrow(cells),
      columns_changed = sum(columns$changed_cells > 0),
      schema_events = sum(is_change),
      rows_shown = nrow(rows),
      rows_total = collected$total
    ),
    truncated = collected$truncated,
    max_rows = if (is.finite(max_rows)) max_rows else NA_integer_
  )
}

check_feed_columns <- function(cols) {
  missing <- setdiff(c("snapshot_id", "rowid", "change_type"), cols)
  if (length(missing) > 0) {
    cli::cli_abort(c(
      "{.arg changes} is not a change feed: missing column{?s} {.val {missing}}.",
      "i" = "Build it with {.fn get_table_changes} and keep {.field snapshot_id}, {.field rowid}, and {.field change_type}."
    ))
  }
  invisible(TRUE)
}

#' Collect a lazy feed in a fixed order, one row past the cap
#'
#' The order keeps an update's two images adjacent and makes the cut
#' deterministic. One extra row tells truncation apart from an exact fit;
#' the count query runs only when the cap was hit.
#' @noRd
collect_change_feed <- function(changes, max_rows) {
  ordered <- dplyr::arrange(
    changes, .data$snapshot_id, .data$rowid, .data$change_type
  )
  rows <- if (is.finite(max_rows)) {
    dplyr::collect(utils::head(ordered, max_rows + 1L))
  } else {
    dplyr::collect(ordered)
  }
  total <- if (nrow(rows) > max_rows) {
    as.numeric(dplyr::collect(dplyr::tally(changes))$n)
  } else {
    nrow(rows)
  }
  cut_change_feed(rows, max_rows, total)
}

#' Cut a feed to the cap without separating an update's two images
#'
#' Sorted by change_type, a pair sits as update_postimage then
#' update_preimage. When the cap lands between them, one fewer row is kept.
#' @noRd
cut_change_feed <- function(rows, max_rows, total) {
  truncated <- nrow(rows) > max_rows
  if (truncated) {
    keep <- max_rows
    is_image <- rows$change_type %in% c("update_preimage", "update_postimage")
    if (is_image[keep] && is_image[keep + 1L] &&
        rows$snapshot_id[keep] == rows$snapshot_id[keep + 1L] &&
        rows$rowid[keep] == rows$rowid[keep + 1L]) {
      keep <- keep - 1L
    }
    rows <- rows[seq_len(keep), , drop = FALSE]
  }
  rownames(rows) <- NULL
  list(rows = rows, truncated = truncated, total = as.numeric(total))
}

drop_unknown_changes <- function(rows) {
  known <- c("insert", "delete", "update_preimage", "update_postimage")
  unknown <- setdiff(unique(rows$change_type), known)
  if (length(unknown) > 0) {
    cli::cli_warn(
      "Ignoring rows with unknown change type{?s} {.val {unknown}}."
    )
    rows <- rows[rows$change_type %in% known, , drop = FALSE]
    rownames(rows) <- NULL
  }
  rows
}

#' One item per change: an insert, a delete, or an update with both images
#'
#' Images pair by snapshot and rowid. A lone image (a dplyr filter on a
#' data column kept one side only) becomes a one-sided update.
#' @returns A data frame with `kind`, `old`, `new` (row positions or NA),
#'   `snapshot_id`, `rowid`, and `partial`.
#' @noRd
pair_change_rows <- function(rows) {
  # A NULL side is NA for every item; both sides given means a complete pair
  item_rows <- function(kind, old = NULL, new = NULL) {
    n <- max(length(old), length(new))
    if (is.null(old)) old <- rep(NA_integer_, n)
    if (is.null(new)) new <- rep(NA_integer_, n)
    data.frame(
      kind = rep_len(kind, n),
      old = as.integer(old),
      new = as.integer(new),
      stringsAsFactors = FALSE
    )
  }
  pre <- which(rows$change_type == "update_preimage")
  post <- which(rows$change_type == "update_postimage")
  key <- paste(rows$snapshot_id, rows$rowid, sep = "\r")
  m <- match(key[pre], key[post])
  paired_pre <- pre[!is.na(m)]
  paired_post <- post[m[!is.na(m)]]

  items <- rbind(
    item_rows("insert", NULL, which(rows$change_type == "insert")),
    item_rows("delete", which(rows$change_type == "delete"), NULL),
    item_rows("update", paired_pre, paired_post),
    item_rows("update", pre[is.na(m)], NULL),
    item_rows("update", NULL, setdiff(post, paired_post))
  )
  src <- ifelse(is.na(items$new), items$old, items$new)
  items$snapshot_id <- rows$snapshot_id[src]
  items$rowid <- rows$rowid[src]
  items$partial <- items$kind == "update" & (is.na(items$old) | is.na(items$new))
  items <- items[order(items$snapshot_id, items$rowid), , drop = FALSE]
  rownames(items) <- NULL
  items[, c("kind", "snapshot_id", "rowid", "old", "new", "partial")]
}

#' The cells that differ between the two images of each complete update
#'
#' Typed values are compared, NA-aware, so `NA` and `NaN` agree and a value
#' never equals `NA`. List and struct columns compare their display text.
#' @returns A data frame with `item` (1-based row of `items`), `column`,
#'   `old`, and `new` (display text).
#' @noRd
changed_cells <- function(rows, items, data, data_cols) {
  pair <- which(items$kind == "update" & !items$partial)
  if (length(pair) == 0 || length(data_cols) == 0) {
    return(empty_cells())
  }
  old_i <- items$old[pair]
  new_i <- items$new[pair]
  found <- lapply(data_cols, function(col) {
    x <- rows[[col]]
    shown <- data[[col]]
    if (is.list(x)) {
      a <- shown[old_i]
      b <- shown[new_i]
    } else {
      a <- x[old_i]
      b <- x[new_i]
      if (is.factor(a)) {
        a <- as.character(a)
        b <- as.character(b)
      }
    }
    na_a <- is.na(a)
    na_b <- is.na(b)
    hit <- which((na_a != na_b) | (!na_a & !na_b & a != b))
    data.frame(
      item = pair[hit],
      column = rep_len(col, length(hit)),
      old = as.character(shown[old_i[hit]]),
      new = as.character(shown[new_i[hit]]),
      stringsAsFactors = FALSE
    )
  })
  cells <- do.call(rbind, c(list(empty_cells()), found))
  cells <- cells[order(cells$item, match(cells$column, data_cols)), , drop = FALSE]
  rownames(cells) <- NULL
  cells
}

empty_cells <- function() {
  data.frame(
    item = integer(0), column = character(0),
    old = character(0), new = character(0),
    stringsAsFactors = FALSE
  )
}

#' The snapshots the viewer lists, with metadata when the table is known
#'
#' With a table, the lake's snapshots in the requested range (all of them,
#' including ones that changed no rows) plus any the feed mentions; without
#' one, the feed's snapshot ids alone.
#' @returns A list with `snapshots` (a data frame), `start_id`, `end_id`.
#' @noRd
feed_snapshots <- function(rows, items, info, has_table, table, schema,
                           ducklake_name, conn) {
  feed_ids <- sort(unique(rows$snapshot_id))
  snaps <- NULL
  if (has_table) {
    snaps <- list_table_snapshots(
      paste0(schema, ".", table), ducklake_name, conn
    )
    if (nrow(snaps) > 0) {
      in_range <- if (is.null(info)) {
        length(feed_ids) > 0 &
          snaps$snapshot_id >= min(feed_ids) & snaps$snapshot_id <= max(feed_ids)
      } else if (info$bound_type == "snapshot") {
        snaps$snapshot_id >= info$start & snaps$snapshot_id <= info$end
      } else {
        snaps$snapshot_time >= as_utc(info$start) &
          snaps$snapshot_time <= as_utc(info$end)
      }
      snaps <- snaps[in_range, , drop = FALSE]
    }
  }
  ids <- sort(unique(c(if (is.null(snaps)) NULL else snaps$snapshot_id, feed_ids)))

  if (!is.null(info) && info$bound_type == "snapshot") {
    start_id <- as.numeric(info$start)
    end_id <- as.numeric(info$end)
  } else if (length(ids) > 0) {
    start_id <- min(ids)
    end_id <- max(ids)
  } else {
    start_id <- NA_real_
    end_id <- NA_real_
  }

  per_kind <- function(kind) {
    as.integer(table(factor(
      items$snapshot_id[items$kind == kind], levels = ids
    )))
  }
  time <- author <- message <- rep(NA_character_, length(ids))
  if (!is.null(snaps) && nrow(snaps) > 0) {
    at <- match(snaps$snapshot_id, ids)
    time[at] <- format(snaps$snapshot_time, "%Y-%m-%d %H:%M:%S", tz = "UTC")
    # snapshots() may omit author/commit_message depending on DuckLake version
    if (!is.null(snaps$author)) author[at] <- as.character(snaps$author)
    if (!is.null(snaps$commit_message)) {
      message[at] <- as.character(snaps$commit_message)
    }
  }
  snapshots <- data.frame(
    snapshot_id = as.numeric(ids),
    time = time,
    author = author,
    commit_message = message,
    inserted = per_kind("insert"),
    updated = per_kind("update"),
    deleted = per_kind("delete"),
    stringsAsFactors = FALSE
  )
  list(snapshots = snapshots, start_id = start_id, end_id = end_id)
}

as_utc <- function(x) {
  if (inherits(x, "POSIXct")) x else as.POSIXct(x, tz = "UTC")
}

format_bound <- function(x) {
  if (inherits(x, "POSIXct")) {
    sub("\\.?0+$", "", format_timestamp(x))
  } else {
    as.character(x)
  }
}

#' Column history of the table's current id inside a snapshot range
#'
#' Every ALTER writes a new version of a column with the same column_id and
#' closes the old one; `end_snapshot` is exclusive, so a version that ends
#' at snapshot N and one that begins at N describe one change made by N.
#' @returns A list with `available`, `reason`, `events`, and `columns`.
#' @noRd
schema_history <- function(table, schema, ducklake_name, conn, start_id, end_id) {
  prefix <- metadata_prefix(ducklake_name, conn)
  sql <- sprintf(
    "SELECT c.column_id, c.begin_snapshot, c.end_snapshot, c.column_order,
            c.column_name, c.column_type, c.nulls_allowed, c.default_value,
            t.begin_snapshot AS table_begin
     FROM %1$s.ducklake_column c
     JOIN %1$s.ducklake_table t
       ON c.table_id = t.table_id AND t.end_snapshot IS NULL
     JOIN %1$s.ducklake_schema s
       ON t.schema_id = s.schema_id AND s.end_snapshot IS NULL
     WHERE t.table_name = ? AND s.schema_name = ?
       AND c.parent_column IS NULL
       AND c.begin_snapshot <= ?
       AND (c.end_snapshot IS NULL OR c.end_snapshot >= ?)
     ORDER BY c.column_id, c.begin_snapshot",
    prefix
  )
  versions <- tryCatch(
    DBI::dbGetQuery(conn, sql, params = list(table, schema, end_id, start_id)),
    error = function(e) e
  )
  if (inherits(versions, "error")) {
    return(list(
      available = FALSE,
      reason = conditionMessage(versions),
      events = empty_schema_events(),
      columns = empty_schema_columns()
    ))
  }
  events <- classify_schema_versions(versions, start_id, end_id)

  current <- versions[
    versions$begin_snapshot <= end_id &
      (is.na(versions$end_snapshot) | versions$end_snapshot > end_id), ,
    drop = FALSE
  ]
  current <- current[order(current$column_order), , drop = FALSE]
  last_kind <- vapply(current$column_id, function(id) {
    kinds <- events$kind[events$column_id == id]
    if (length(kinds) == 0) "unchanged" else kinds[length(kinds)]
  }, character(1))
  dropped <- events[events$kind == "dropped" &
                      !(events$column_id %in% current$column_id), , drop = FALSE]
  columns <- rbind(
    data.frame(
      name = as.character(current$column_name),
      type = as.character(current$column_type),
      status = last_kind,
      stringsAsFactors = FALSE
    ),
    data.frame(
      name = as.character(dropped$old_name),
      type = as.character(dropped$old_type),
      status = rep_len("dropped", nrow(dropped)),
      stringsAsFactors = FALSE
    )
  )
  rownames(columns) <- NULL
  list(available = TRUE, reason = NA_character_, events = events, columns = columns)
}

#' Turn column versions into one event per column per snapshot
#' @noRd
classify_schema_versions <- function(v, start_id, end_id) {
  if (nrow(v) == 0) {
    return(empty_schema_events())
  }
  event <- function(snapshot, kind, id, name, detail, old_name, new_name,
                    old_type, new_type) {
    data.frame(
      snapshot_id = as.numeric(snapshot), kind = kind,
      column_id = as.numeric(id), column_name = name, detail = detail,
      old_name = old_name, new_name = new_name,
      old_type = old_type, new_type = new_type,
      stringsAsFactors = FALSE
    )
  }
  ends <- v$end_snapshot[!is.na(v$end_snapshot)]
  snapshots <- sort(unique(c(v$begin_snapshot, ends)))
  snapshots <- snapshots[snapshots >= start_id & snapshots <= end_id]
  out <- list(empty_schema_events())
  for (n in snapshots) {
    begun <- v[v$begin_snapshot == n, , drop = FALSE]
    ended <- v[!is.na(v$end_snapshot) & v$end_snapshot == n, , drop = FALSE]
    both <- intersect(begun$column_id, ended$column_id)
    for (id in both) {
      a <- ended[ended$column_id == id, , drop = FALSE][1, ]
      b <- begun[begun$column_id == id, , drop = FALSE][1, ]
      if (!identical(a$column_name, b$column_name)) {
        kind <- "renamed"
        detail <- paste(a$column_name, "->", b$column_name)
      } else if (!identical(a$column_type, b$column_type)) {
        kind <- "type_changed"
        detail <- paste(a$column_type, "->", b$column_type)
      } else if (!identical(a$nulls_allowed, b$nulls_allowed)) {
        kind <- "altered"
        detail <- if (isTRUE(b$nulls_allowed)) "NULL allowed" else "NOT NULL"
      } else if (!identical(a$default_value, b$default_value)) {
        kind <- "altered"
        detail <- paste("default", a$default_value, "->", b$default_value)
      } else {
        kind <- "altered"
        detail <- ""
      }
      out[[length(out) + 1]] <- event(
        n, kind, id, b$column_name, detail,
        a$column_name, b$column_name, a$column_type, b$column_type
      )
    }
    for (id in setdiff(begun$column_id, both)) {
      b <- begun[begun$column_id == id, , drop = FALSE][1, ]
      kind <- if (isTRUE(n == b$table_begin)) "created" else "added"
      out[[length(out) + 1]] <- event(
        n, kind, id, b$column_name, b$column_type,
        NA_character_, b$column_name, NA_character_, b$column_type
      )
    }
    for (id in setdiff(ended$column_id, both)) {
      a <- ended[ended$column_id == id, , drop = FALSE][1, ]
      out[[length(out) + 1]] <- event(
        n, "dropped", id, a$column_name, a$column_type,
        a$column_name, NA_character_, a$column_type, NA_character_
      )
    }
  }
  events <- do.call(rbind, out)
  rownames(events) <- NULL
  events
}

empty_schema_events <- function() {
  data.frame(
    snapshot_id = numeric(0), kind = character(0), column_id = numeric(0),
    column_name = character(0), detail = character(0),
    old_name = character(0), new_name = character(0),
    old_type = character(0), new_type = character(0),
    stringsAsFactors = FALSE
  )
}

empty_schema_columns <- function() {
  data.frame(
    name = character(0), type = character(0), status = character(0),
    stringsAsFactors = FALSE
  )
}

#' Display text for one feed column, NA kept as NA
#'
#' duckdb returns a STRUCT as a data.frame column, a LIST as a list column,
#' a BLOB as a list of raw vectors, TIME as difftime, and BIGINT, HUGEINT,
#' and DECIMAL as numeric.
#' @noRd
format_cell <- function(x) {
  if (is.data.frame(x)) {
    fields <- lapply(x, format_cell)
    return(vapply(seq_len(nrow(x)), function(i) {
      vals <- vapply(fields, function(f) f[[i]], character(1))
      vals[is.na(vals)] <- "NA"
      paste0("{", paste0(names(x), ": ", vals, collapse = ", "), "}")
    }, character(1)))
  }
  if (is.list(x)) {
    return(vapply(x, function(el) {
      if (is.null(el) || (is.atomic(el) && !is.raw(el) && length(el) == 1 && is.na(el))) {
        return(NA_character_)
      }
      if (is.raw(el)) {
        return(paste(format(el), collapse = ""))
      }
      inner <- format_cell(el)
      inner[is.na(inner)] <- "NA"
      paste0("[", paste(inner, collapse = ", "), "]")
    }, character(1)))
  }
  if (is.factor(x)) {
    return(as.character(x))
  }
  if (inherits(x, "POSIXct")) {
    return(sub("\\.?0+$", "", format(x, "%Y-%m-%d %H:%M:%OS6", tz = "UTC")))
  }
  if (inherits(x, "Date")) {
    return(format(x, "%Y-%m-%d"))
  }
  if (inherits(x, "difftime")) {
    secs <- as.numeric(x, units = "secs")
    whole <- floor(secs)
    out <- sprintf(
      "%02d:%02d:%02d",
      as.integer(whole %/% 3600), as.integer((whole %% 3600) %/% 60),
      as.integer(whole %% 60)
    )
    frac <- secs - whole
    has_frac <- !is.na(frac) & frac > 0
    out[has_frac] <- paste0(
      out[has_frac], substring(sub("0+$", "", sprintf("%.6f", frac[has_frac])), 2)
    )
    out[is.na(secs)] <- NA_character_
    return(out)
  }
  if (is.logical(x)) {
    return(ifelse(is.na(x), NA_character_, ifelse(x, "TRUE", "FALSE")))
  }
  as.character(x)
}

simple_type_label <- function(x) {
  if (is.data.frame(x)) {
    "struct"
  } else if (is.list(x)) {
    if (any(vapply(x, is.raw, logical(1)))) "blob" else "list"
  } else if (inherits(x, "POSIXct")) {
    "timestamp"
  } else if (inherits(x, "Date")) {
    "date"
  } else if (inherits(x, "difftime")) {
    "time"
  } else if (is.logical(x)) {
    "logical"
  } else if (is.integer(x)) {
    "integer"
  } else if (is.numeric(x)) {
    "number"
  } else {
    "text"
  }
}
