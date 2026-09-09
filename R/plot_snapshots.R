#' Plot the snapshot history of a table or lake
#'
#' Draws snapshot history in one of two layouts. With a `table_name`, a
#' commit-log timeline: one row per snapshot (newest at top) on an ordinal
#' spine, with the timestamp, author, and commit message as aligned text and
#' long idle stretches marked inline (e.g. "103 days later") instead of
#' stretching an axis. Without a `table_name`, a lake-wide swimlane: one row
#' per table, one point per snapshot, evenly spaced in snapshot order, so
#' active and stale tables read at a glance.
#'
#' @param table_name The name of the table to plot. If NULL, plots all
#'   snapshots in the ducklake.
#' @param ducklake_name The name of the ducklake (database) to query. If NULL, will attempt to infer from current database.
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns A ggplot object, which can be further customized with ggplot2
#'   functions
#' @family time travel
#' @export
#'
#' @details
#' Requires the ggplot2 package (listed in Suggests). Snapshot data comes from
#' [list_table_snapshots()]; each snapshot is classified from the entries of
#' its `changes` column that name the table into one of: created, schema
#' change, data change, maintenance, or other. A transaction that updates
#' one table in place and rebuilds another therefore shows a data change on
#' the first and a creation on the second. Authors and commit messages
#' appear where they were recorded (see [set_snapshot_metadata()] and
#' [commit_transaction()]).
#'
#' Both layouts position snapshots by order rather than by clock time, so a
#' history with months of silence between bursts of activity stays readable.
#' In the swimlane, snapshots that touch no table (like the initial schema
#' creation) appear in a `(lake)` lane, and the x axis labels show each
#' snapshot's date. Lanes carry schema-qualified names when the lake keeps
#' tables outside `main`, so `bronze.dm` and `silver.dm` stay apart.
#'
#'
#' @examplesIf ducklake_extension_available() && requireNamespace("ggplot2", quietly = TRUE)
#' lake_dir <- tempfile("plotsnap_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("plotsnap_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' create_table(iris, "flowers")
#'
#' # Commit-log timeline of one table's history
#' plot_snapshots("cars")
#'
#' # Swimlane of every table in the lake
#' plot_snapshots()
#'
#' # Customize the result like any ggplot
#' plot_snapshots("cars") +
#'   ggplot2::labs(title = "Audit trail")
#'
#' detach_ducklake("plotsnap_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
plot_snapshots <- function(table_name = NULL, ducklake_name = NULL, conn = NULL) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort(c(
      "{.pkg ggplot2} is required for {.fn plot_snapshots}.",
      "i" = "Install it with {.code install.packages(\"ggplot2\")}."
    ))
  }

  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  snapshots <- list_table_snapshots(table_name, ducklake_name, conn)
  if (nrow(snapshots) == 0) {
    target <- if (is.null(table_name)) "this ducklake" else table_name
    cli::cli_abort(c(
      "No snapshots found for {.val {target}}.",
      "i" = "Make sure the ducklake is attached and has snapshots."
    ))
  }

  snapshots <- snapshots[order(snapshots$snapshot_time, snapshots$snapshot_id), ]

  if (!is.null(table_name)) {
    # Classify each snapshot by what it did to this table, not by everything
    # the transaction touched
    targets <- table_change_targets(table_name, ducklake_name, conn)
    snapshots$change_type <- classify_snapshot_changes(snapshots$changes, targets)
    plot_snapshot_commit_log(snapshots, table_name)
  } else {
    plot_snapshot_swimlane(snapshots, ducklake_name, conn)
  }
}

# Okabe-Ito hues (colorblind-safe); fixed name-to-color mapping so a
# category keeps its color regardless of which categories are present
snapshot_change_colors <- c(
  "created" = "#0072B2",
  "schema change" = "#E69F00",
  "data change" = "#009E73",
  "maintenance" = "#56B4E9",
  "other" = "#CC79A7"
)

snapshot_history_subtitle <- function(snapshots) {
  sprintf(
    "%d snapshot%s from %s to %s (UTC)",
    nrow(snapshots),
    if (nrow(snapshots) == 1) "" else "s",
    format(min(snapshots$snapshot_time), "%Y-%m-%d %H:%M:%S"),
    format(max(snapshots$snapshot_time), "%Y-%m-%d %H:%M:%S")
  )
}

#' Commit-log layout: ordinal spine, time as text, inline gap markers
#'
#' @noRd
plot_snapshot_commit_log <- function(snapshots, table_name) {
  d <- snapshots
  d$row <- seq_len(nrow(d))
  d$id_label <- paste0("#", d$snapshot_id)
  d$time_label <- format(d$snapshot_time, "%Y-%m-%d %H:%M:%S")

  # snapshots() may omit author/commit_message depending on DuckLake version
  author <- if (is.null(d$author)) rep("", nrow(d)) else ifelse(is.na(d$author), "", d$author)
  msg <- if (is.null(d$commit_message)) rep("", nrow(d)) else ifelse(is.na(d$commit_message), "", d$commit_message)
  annotation <- ifelse(
    author != "" & msg != "",
    paste0(author, ": ", msg),
    paste0(author, msg)
  )
  d$annotation <- ifelse(annotation == "", NA_character_, annotation)

  # A gap gets an inline marker when it is both long in absolute terms and
  # an outlier for this table's cadence, so scripted bursts of commits don't
  # trigger markers while real idle stretches do
  gap_secs <- diff(as.numeric(d$snapshot_time))
  gaps <- data.frame(y = utils::head(d$row, -1) + 0.5, secs = gap_secs)
  gaps <- gaps[!is.na(gaps$secs), , drop = FALSE]
  if (nrow(gaps) > 0) {
    gaps <- gaps[
      gaps$secs > max(4 * stats::median(gaps$secs), 3600), ,
      drop = FALSE
    ]
  }

  # Fixed x positions lay the id, timestamp, and annotation out as columns;
  # the coordinates are arbitrary units within xlim
  x_spine <- 0
  x_id <- -0.15
  x_time <- 0.2
  x_annotation <- 1.7

  p <- ggplot2::ggplot(d, ggplot2::aes(x = x_spine, y = .data$row)) +
    ggplot2::annotate(
      "segment",
      x = x_spine, xend = x_spine, y = 1, yend = nrow(d),
      color = "grey80", linewidth = 0.4
    ) +
    ggplot2::geom_point(ggplot2::aes(color = .data$change_type), size = 3) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$id_label),
      x = x_id, hjust = 1, size = 3, color = "grey40"
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$time_label),
      x = x_time, hjust = 0, size = 3, color = "grey40", family = "mono"
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$annotation),
      x = x_annotation, hjust = 0, size = 3, color = "grey20", na.rm = TRUE
    )

  if (nrow(gaps) > 0) {
    gaps$label <- paste0(
      "\u2500\u2500  ", vapply(gaps$secs, format_gap_duration, character(1)),
      " later  \u2500\u2500"
    )
    p <- p + ggplot2::geom_text(
      data = gaps,
      ggplot2::aes(x = x_time, y = .data$y, label = .data$label),
      inherit.aes = FALSE, hjust = 0, size = 2.8, color = "grey55"
    )
  }

  p +
    ggplot2::scale_x_continuous(limits = c(-0.6, 4.5)) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(add = 0.6)) +
    ggplot2::scale_color_manual(values = snapshot_change_colors, drop = FALSE) +
    ggplot2::labs(
      title = sprintf("Snapshot history of %s", table_name),
      subtitle = snapshot_history_subtitle(snapshots),
      color = "Change type"
    ) +
    ggplot2::theme_void() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold"),
      plot.subtitle = ggplot2::element_text(color = "grey30"),
      plot.margin = ggplot2::margin(10, 10, 10, 10)
    )
}

#' Swimlane layout: one lane per table, points in snapshot order
#'
#' @noRd
plot_snapshot_swimlane <- function(snapshots, ducklake_name, conn) {
  snapshots$order <- seq_len(nrow(snapshots))
  id_names <- ducklake_table_id_names(ducklake_name, conn)
  attributed <- snapshot_table_changes(snapshots$changes, id_names)

  d <- snapshots[
    rep(seq_len(nrow(snapshots)), vapply(attributed, nrow, integer(1))),
  ]
  attributed <- do.call(rbind, attributed)
  d$table <- display_table_names(attributed$table)
  d$change_type <- factor(
    attributed$change_type,
    levels = names(snapshot_change_colors)
  )

  # Lanes ordered by last activity so recently active tables sit on top and
  # stale ones sink to the bottom
  last_active <- tapply(d$order, d$table, max)
  d$table <- factor(d$table, levels = names(sort(last_active)))

  # Ordinal x keeps bursts readable regardless of gaps; date labels at the
  # breaks carry the calendar information instead of the spacing
  breaks <- unique(round(pretty(snapshots$order, n = 6)))
  breaks <- breaks[breaks >= 1 & breaks <= nrow(snapshots)]
  break_labels <- format(snapshots$snapshot_time[breaks], "%b %d")

  ggplot2::ggplot(d, ggplot2::aes(x = .data$order, y = .data$table)) +
    ggplot2::geom_point(
      ggplot2::aes(color = .data$change_type), size = 3, alpha = 0.8
    ) +
    ggplot2::scale_x_continuous(breaks = breaks, labels = break_labels) +
    ggplot2::scale_color_manual(values = snapshot_change_colors, drop = FALSE) +
    ggplot2::labs(
      title = sprintf("Snapshot history of %s", ducklake_name),
      subtitle = snapshot_history_subtitle(snapshots),
      x = "Snapshot (in order, labeled by date)",
      y = NULL,
      color = "Change type"
    ) +
    ggplot2::theme_minimal()
}

#' Human-readable duration for gap markers
#'
#' @noRd
format_gap_duration <- function(secs) {
  if (secs < 3600) {
    sprintf("%.0f minutes", secs / 60)
  } else if (secs < 48 * 3600) {
    sprintf("%.0f hours", secs / 3600)
  } else {
    sprintf("%.0f days", secs / 86400)
  }
}

#' Current qualified name for each table id in the lake's metadata catalog
#'
#' Returns a named character vector mapping table_id to `schema.table`.
#' Renamed or replaced tables have several metadata rows per id; the one
#' with the largest begin_snapshot holds the most recent name.
#'
#' @noRd
ducklake_table_id_names <- function(ducklake_name, conn) {
  prefix <- metadata_prefix(ducklake_name, conn)
  tables <- tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT t.table_id, s.schema_name || '.' || t.table_name AS table_name
         FROM %s.ducklake_table t
         JOIN %s.ducklake_schema s ON t.schema_id = s.schema_id
         ORDER BY t.begin_snapshot, s.begin_snapshot",
        prefix, prefix
      )
    ),
    error = function(e) data.frame(table_id = numeric(0), table_name = character(0))
  )
  # Later rows overwrite earlier ones, leaving the most recent name per id
  stats::setNames(tables$table_name, tables$table_id)[
    !duplicated(tables$table_id, fromLast = TRUE)
  ]
}

#' Attribute each snapshot's changes to tables, each with its own change type
#'
#' Values under table-related change keys are either qualified names
#' ("main.fleet", from creates and renames) or numeric table ids ("1", from
#' row changes, alters, drops, and flushes), so ids go through the metadata
#' map to their qualified name. A table's change type comes from the entries
#' that name it, so one transaction can leave a data change on one table and
#' a creation on another. Snapshots with no table attribution (like schema
#' creation) fall into a "(lake)" lane classified from the whole entry.
#'
#' @param changes The list-column from snapshots(): one data.frame of
#'   key/value pairs per snapshot.
#' @param id_names Named character vector from ducklake_table_id_names().
#' @returns A list of data frames, one per snapshot, with columns `table`
#'   and `change_type`.
#' @noRd
snapshot_table_changes <- function(changes, id_names) {
  lake_lane <- function(entry) {
    data.frame(
      table = "(lake)",
      change_type = classify_change_tokens(change_keys(entry)),
      stringsAsFactors = FALSE
    )
  }
  attribute_one <- function(entry) {
    if (!is.data.frame(entry) || nrow(entry) == 0) {
      return(lake_lane(entry))
    }
    keep <- !startsWith(as.character(entry$key), "schemas")
    values <- unique(unlist(entry$value[keep], use.names = FALSE))
    values <- as.character(values[!is.na(values)])
    if (length(values) == 0) {
      return(lake_lane(entry))
    }
    lanes <- resolve_change_values(values, id_names)
    rows <- lapply(unique(lanes), function(lane) {
      data.frame(
        table = lane,
        change_type = classify_change_tokens(
          change_keys(entry, names(lanes)[lanes == lane])
        ),
        stringsAsFactors = FALSE
      )
    })
    do.call(rbind, rows)
  }
  lapply(changes, attribute_one)
}

#' Map change-map values to table names
#'
#' Ids go through the metadata map; names stay as they are. An id the
#' catalog does not know keeps a placeholder lane rather than being dropped.
#' The result is named by the original values, so the caller can find the
#' values behind each lane.
#' @noRd
resolve_change_values <- function(values, id_names) {
  is_id <- grepl("^[0-9]+$", values)
  resolved <- values
  resolved[is_id] <- ifelse(
    values[is_id] %in% names(id_names),
    id_names[values[is_id]],
    paste("table", values[is_id])
  )
  stats::setNames(resolved, values)
}

#' Lane labels for the swimlane
#'
#' Bare names when every table lives in `main`, schema-qualified names
#' otherwise, so same-named tables in different schemas keep separate lanes.
#' @noRd
display_table_names <- function(x) {
  qualified <- grepl(".", x, fixed = TRUE)
  schemas <- unique(sub("\\.[^.]*$", "", x[qualified]))
  if (length(schemas) <= 1 && all(schemas == "main")) {
    x[qualified] <- sub("^main\\.", "", x[qualified])
  }
  x
}

#' Classify snapshots' changes into change categories
#'
#' The changes column from snapshots() arrives as a list column where each
#' element is a data.frame of key/value pairs (keys are change tokens like
#' "tables_created"); character input such as
#' "tables_created, tables_inserted_into, main.my_table, 1" is also accepted.
#' Categories are checked in priority order; unrecognized tokens fall through
#' to "other".
#'
#' @param targets Optional character vector of the change-map values that
#'   identify one table (its qualified name and every id it has had, from
#'   table_change_targets()). When given, only the entries that name that
#'   table count, so a snapshot is classified by what it did to the table
#'   rather than by everything the transaction touched.
#' @noRd
classify_snapshot_changes <- function(changes, targets = NULL) {
  if (!is.list(changes)) {
    changes <- as.list(changes)
  }
  factor(
    vapply(
      changes,
      function(entry) classify_change_tokens(change_keys(entry, targets)),
      character(1),
      USE.NAMES = FALSE
    ),
    levels = names(snapshot_change_colors)
  )
}

#' The change tokens of one snapshot entry
#'
#' For a key/value data.frame, the keys, optionally only those whose values
#' include one of `targets`; character input is returned as is.
#' @noRd
change_keys <- function(entry, targets = NULL) {
  if (!is.data.frame(entry)) {
    return(unlist(entry))
  }
  keys <- as.character(entry$key)
  if (is.null(targets)) {
    return(keys)
  }
  named <- vapply(
    entry$value,
    function(v) any(as.character(v) %in% targets),
    logical(1)
  )
  keys[named]
}

#' One change category for a set of change tokens, in priority order
#' @noRd
classify_change_tokens <- function(tokens) {
  if (length(tokens) == 0 || all(is.na(tokens))) {
    return("other")
  }
  x <- paste(tokens, collapse = ", ")
  if (grepl("\\btables_created\\b|\\bschemas_created\\b", x)) {
    return("created")
  }
  if (grepl("\\btables_altered\\b|\\btables_dropped\\b", x)) {
    return("schema change")
  }
  if (grepl(
    "\\btables_inserted_into\\b|\\btables_deleted_from\\b|\\binlined_insert\\b|\\binlined_delete\\b",
    x
  )) {
    return("data change")
  }
  if (grepl("\\bflushed_inlined\\b|\\bcompacted\\b", x)) {
    return("maintenance")
  }
  "other"
}
