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
#' [list_table_snapshots()]; each snapshot is classified from its `changes`
#' column into one of: created, schema change, data change, maintenance, or
#' other. Authors and commit messages appear where they were recorded (see
#' [set_snapshot_metadata()] and [commit_transaction()]).
#'
#' Both layouts position snapshots by order rather than by clock time, so a
#' history with months of silence between bursts of activity stays readable.
#' In the swimlane, snapshots that touch no table (like the initial schema
#' creation) appear in a `(lake)` lane, and the x axis labels show each
#' snapshot's date.
#'
#' @importFrom dplyr .data
#'
#' @examples
#' \dontrun{
#' # Commit-log timeline of one table's history
#' plot_snapshots("my_table")
#'
#' # Swimlane of every table in the lake
#' plot_snapshots()
#'
#' # Customize the result like any ggplot
#' plot_snapshots("my_table") +
#'   ggplot2::labs(title = "Audit trail")
#' }
plot_snapshots <- function(table_name = NULL, ducklake_name = NULL, conn = NULL) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort(c(
      "{.pkg ggplot2} is required for {.fn plot_snapshots}.",
      "i" = "Install it with {.code install.packages(\"ggplot2\")}."
    ))
  }

  snapshots <- list_table_snapshots(table_name, ducklake_name, conn)
  if (nrow(snapshots) == 0) {
    target <- if (is.null(table_name)) "this ducklake" else table_name
    cli::cli_abort(c(
      "No snapshots found for {.val {target}}.",
      "i" = "Make sure the ducklake is attached and has snapshots."
    ))
  }

  snapshots$change_type <- classify_snapshot_changes(snapshots$changes)
  snapshots <- snapshots[order(snapshots$snapshot_time, snapshots$snapshot_id), ]

  if (!is.null(table_name)) {
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

  author <- ifelse(is.na(d$author), "", d$author)
  msg <- ifelse(is.na(d$commit_message), "", d$commit_message)
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
  gaps <- gaps[
    gaps$secs > max(4 * stats::median(gap_secs), 3600) & !is.na(gaps$secs),
  ]

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
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  snapshots$order <- seq_len(nrow(snapshots))
  id_names <- ducklake_table_id_names(ducklake_name, conn)
  tables <- snapshot_change_tables(snapshots$changes, id_names)

  d <- snapshots[rep(seq_len(nrow(snapshots)), lengths(tables)), ]
  d$table <- unlist(tables)

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

#' Current name for each table id in the lake's metadata catalog
#'
#' Returns a named character vector mapping table_id to table_name. Renamed
#' or replaced tables have several metadata rows per id; the one with the
#' largest begin_snapshot holds the most recent name.
#'
#' @noRd
ducklake_table_id_names <- function(ducklake_name, conn) {
  backend <- get_ducklake_backend()
  metadata_ref <- if (backend %in% c("postgres", "mysql")) {
    paste0("__ducklake_metadata_", ducklake_name, ".ducklake_table")
  } else {
    paste0("__ducklake_metadata_", ducklake_name, ".main.ducklake_table")
  }
  tables <- tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT table_id, table_name FROM %s ORDER BY begin_snapshot",
        quote_ident(metadata_ref, conn)
      )
    ),
    error = function(e) data.frame(table_id = numeric(0), table_name = character(0))
  )
  # Later rows overwrite earlier ones, leaving the most recent name per id
  stats::setNames(tables$table_name, tables$table_id)[
    !duplicated(tables$table_id, fromLast = TRUE)
  ]
}

#' Attribute each snapshot's changes to table names
#'
#' Values under table-related change keys are either qualified names
#' ("main.fleet", from creates) or numeric table ids ("1", from DML), so ids
#' go through the metadata map and names get their schema prefix stripped.
#' Snapshots with no table attribution (like schema creation) fall into a
#' "(lake)" lane.
#'
#' @param changes The list-column from snapshots(): one data.frame of
#'   key/value pairs per snapshot.
#' @param id_names Named character vector from ducklake_table_id_names().
#' @returns A list of character vectors, one per snapshot.
#' @noRd
snapshot_change_tables <- function(changes, id_names) {
  attribute_one <- function(entry) {
    if (!is.data.frame(entry) || nrow(entry) == 0) {
      return("(lake)")
    }
    keep <- !startsWith(entry$key, "schemas")
    values <- unique(unlist(entry$value[keep]))
    values <- values[!is.na(values)]
    if (length(values) == 0) {
      return("(lake)")
    }
    is_id <- grepl("^[0-9]+$", values)
    mapped <- character(length(values))
    mapped[is_id] <- ifelse(
      values[is_id] %in% names(id_names),
      id_names[values[is_id]],
      paste("table", values[is_id])
    )
    mapped[!is_id] <- sub("^.*\\.", "", values[!is_id])
    unique(mapped)
  }
  lapply(changes, attribute_one)
}

#' Classify a snapshot's changes into a change category
#'
#' The changes column from snapshots() arrives as a list column where each
#' element is a data.frame of key/value pairs (keys are change tokens like
#' "tables_created"); character input such as
#' "tables_created, tables_inserted_into, main.my_table, 1" is also accepted.
#' Categories are checked in priority order; unrecognized tokens fall through
#' to "other".
#'
#' @noRd
classify_snapshot_changes <- function(changes) {
  if (!is.list(changes)) {
    changes <- as.list(changes)
  }
  classify_one <- function(tokens) {
    tokens <- if (is.data.frame(tokens)) tokens$key else unlist(tokens)
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
  factor(
    vapply(changes, classify_one, character(1), USE.NAMES = FALSE),
    levels = c("created", "schema change", "data change", "maintenance", "other")
  )
}
