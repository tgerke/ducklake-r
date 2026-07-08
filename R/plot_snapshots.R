#' Plot the snapshot history of a table or lake
#'
#' Draws a table's snapshot history as a commit-log style timeline: one row per
#' snapshot (newest at top), positioned by snapshot time, colored by the kind
#' of change, and annotated with the author and commit message where those were
#' recorded (see [set_snapshot_metadata()] and [commit_transaction()]).
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
#' other.
#'
#' @importFrom dplyr .data
#'
#' @examples
#' \dontrun{
#' # Plot the snapshot history of a table
#' plot_snapshots("my_table")
#'
#' # Plot every snapshot in the lake
#' plot_snapshots()
#'
#' # Customize the result like any ggplot
#' plot_snapshots("my_table") +
#'   ggplot2::theme_classic()
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
  # Ascending factor levels put the newest snapshot at the top of the y axis
  snapshots$snapshot_label <- factor(
    snapshots$snapshot_id,
    levels = sort(unique(snapshots$snapshot_id))
  )

  author <- ifelse(is.na(snapshots$author), "", snapshots$author)
  msg <- ifelse(is.na(snapshots$commit_message), "", snapshots$commit_message)
  annotation <- ifelse(
    author != "" & msg != "",
    paste0(author, ": ", msg),
    paste0(author, msg)
  )
  snapshots$annotation <- ifelse(annotation == "", NA_character_, annotation)

  # Labels for points in the right 40% of the timeline go on the left of the
  # point so they don't get clipped at the panel edge
  time_range <- range(snapshots$snapshot_time)
  label_cutoff <- time_range[1] + 0.6 * diff(as.numeric(time_range))
  snapshots$label_hjust <- ifelse(
    as.numeric(snapshots$snapshot_time) > label_cutoff, 1.15, -0.15
  )

  # Okabe-Ito hues (colorblind-safe); fixed name-to-color mapping so a
  # category keeps its color regardless of which categories are present
  change_colors <- c(
    "created" = "#0072B2",
    "schema change" = "#E69F00",
    "data change" = "#009E73",
    "maintenance" = "#56B4E9",
    "other" = "#CC79A7"
  )

  title <- if (!is.null(table_name)) {
    sprintf("Snapshot history of %s", table_name)
  } else if (!is.null(ducklake_name)) {
    sprintf("Snapshot history of %s", ducklake_name)
  } else {
    "Snapshot history"
  }
  subtitle <- sprintf(
    "%d snapshot%s from %s to %s (UTC)",
    nrow(snapshots),
    if (nrow(snapshots) == 1) "" else "s",
    format(min(snapshots$snapshot_time), "%Y-%m-%d %H:%M:%S"),
    format(max(snapshots$snapshot_time), "%Y-%m-%d %H:%M:%S")
  )

  ggplot2::ggplot(
    snapshots,
    ggplot2::aes(x = .data$snapshot_time, y = .data$snapshot_label)
  ) +
    ggplot2::geom_line(ggplot2::aes(group = 1), color = "grey70") +
    ggplot2::geom_point(ggplot2::aes(color = .data$change_type), size = 3) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$annotation, hjust = .data$label_hjust),
      size = 3, color = "grey30", na.rm = TRUE
    ) +
    # Expansion on both sides leaves room for the annotation text
    ggplot2::scale_x_datetime(expand = ggplot2::expansion(mult = c(0.15, 0.3))) +
    ggplot2::scale_color_manual(values = change_colors) +
    ggplot2::labs(
      title = title,
      subtitle = subtitle,
      x = "Snapshot time (UTC)",
      y = "Snapshot",
      color = "Change type"
    ) +
    ggplot2::theme_minimal()
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
