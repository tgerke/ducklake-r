#' Plot the rows changed in each snapshot of a table
#'
#' Draws a table's change volume as a diverging bar chart: one bar per
#' snapshot, with rows inserted or updated above the axis and rows deleted
#' below it. A companion to [plot_snapshots()], which shows *when* and *what
#' kind* of changes happened; this shows *how much* changed each time.
#'
#' @param table_name The name of the table to plot.
#' @param ducklake_name The name of the ducklake (database) to query. If NULL, will attempt to infer from current database.
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns A ggplot object, which can be further customized with ggplot2
#'   functions
#' @family time travel
#' @export
#'
#' @details
#' Requires the ggplot2 package (listed in Suggests). Row counts come from
#' DuckLake's data change feed via [get_table_changes()]. An update appears
#' in the feed as a before and an after image of the row; it is counted once
#' here. Snapshots that touched the table without changing rows (a schema
#' change, for example) keep their slot on the axis with no bar.
#'
#' @importFrom dplyr .data
#'
#' @examples
#' \dontrun{
#' # Rows inserted, updated, and deleted per snapshot
#' plot_table_changes("my_table")
#'
#' # Customize the result like any ggplot
#' plot_table_changes("my_table") +
#'   ggplot2::theme_classic()
#' }
plot_table_changes <- function(table_name, ducklake_name = NULL, conn = NULL) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort(c(
      "{.pkg ggplot2} is required for {.fn plot_table_changes}.",
      "i" = "Install it with {.code install.packages(\"ggplot2\")}."
    ))
  }

  snapshots <- list_table_snapshots(table_name, ducklake_name, conn)
  if (nrow(snapshots) == 0) {
    cli::cli_abort(c(
      "No snapshots found for {.val {table_name}}.",
      "i" = "Make sure the ducklake is attached and has snapshots."
    ))
  }

  changes <- get_table_changes(
    table_name,
    min(snapshots$snapshot_id), max(snapshots$snapshot_id),
    ducklake_name, conn
  ) |>
    dplyr::count(.data$snapshot_id, .data$change_type) |>
    dplyr::collect()

  # Count updates once, from the after image
  kind_map <- c(
    "insert" = "inserted",
    "update_postimage" = "updated",
    "delete" = "deleted"
  )
  changes <- changes[changes$change_type %in% names(kind_map), ]
  if (nrow(changes) == 0) {
    cli::cli_abort("No row changes found for {.val {table_name}}.")
  }
  changes$kind <- factor(
    kind_map[changes$change_type],
    levels = c("inserted", "updated", "deleted")
  )
  # Deletions plot below the axis
  changes$rows <- ifelse(changes$kind == "deleted", -changes$n, changes$n)
  # Levels cover every snapshot so row-free snapshots keep their slot
  changes$snapshot_label <- factor(
    changes$snapshot_id,
    levels = sort(unique(snapshots$snapshot_id))
  )

  # Okabe-Ito hues (colorblind-safe); fixed name-to-color mapping so a
  # category keeps its color regardless of which categories are present
  kind_colors <- c(
    "inserted" = "#009E73",
    "updated" = "#0072B2",
    "deleted" = "#D55E00"
  )

  totals <- tapply(changes$n, changes$kind, sum, default = 0)
  subtitle <- sprintf(
    "%d inserted, %d updated, %d deleted across %d snapshot%s",
    totals[["inserted"]], totals[["updated"]], totals[["deleted"]],
    nrow(snapshots),
    if (nrow(snapshots) == 1) "" else "s"
  )

  ggplot2::ggplot(
    changes,
    ggplot2::aes(x = .data$snapshot_label, y = .data$rows, fill = .data$kind)
  ) +
    ggplot2::geom_col(width = 0.7) +
    ggplot2::geom_hline(yintercept = 0, color = "grey30", linewidth = 0.3) +
    ggplot2::scale_x_discrete(drop = FALSE) +
    # The sign is layout only; label the axis with magnitudes
    ggplot2::scale_y_continuous(labels = function(x) abs(x)) +
    ggplot2::scale_fill_manual(values = kind_colors, drop = FALSE) +
    ggplot2::labs(
      title = sprintf("Row changes in %s", table_name),
      subtitle = subtitle,
      x = "Snapshot",
      y = "Rows changed",
      fill = NULL
    ) +
    ggplot2::theme_minimal()
}
