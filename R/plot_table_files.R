#' Plot the file layout of a lake
#'
#' Draws each table's storage footprint as a horizontal bar: total bytes of
#' Parquet data files, with delete files stacked in a second color, and a
#' label giving the file count and average file size. Useful for spotting
#' fragmentation -- many small files -- before it slows scans down.
#'
#' @param ducklake_name The name of the ducklake (database) to query. If NULL, will attempt to infer from current database.
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns A ggplot object, which can be further customized with ggplot2
#'   functions
#' @family maintenance
#' @export
#'
#' @details
#' Requires the ggplot2 package (listed in Suggests). File statistics come
#' from [get_table_info()]. Tables whose rows are still inlined in the
#' catalog have no data files yet and show an empty bar; run
#' [flush_inlined_data()] to write them out. Many small files can be
#' compacted with [merge_adjacent_files()], and a large delete-file share is
#' a sign to run [rewrite_data_files()].
#'
#' @importFrom dplyr .data
#'
#' @examples
#' \dontrun{
#' # File counts and sizes for every table in the lake
#' plot_table_files()
#'
#' # Customize the result like any ggplot
#' plot_table_files() +
#'   ggplot2::theme_classic()
#' }
plot_table_files <- function(ducklake_name = NULL, conn = NULL) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort(c(
      "{.pkg ggplot2} is required for {.fn plot_table_files}.",
      "i" = "Install it with {.code install.packages(\"ggplot2\")}."
    ))
  }

  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  info <- get_table_info(ducklake_name = ducklake_name, conn = conn)
  if (nrow(info) == 0) {
    cli::cli_abort(c(
      "No tables found in {.val {ducklake_name}}.",
      "i" = "Make sure the ducklake is attached and has tables."
    ))
  }

  # Ascending levels put the largest table at the top of the y axis
  info$total_bytes <- info$file_size_bytes + info$delete_file_size_bytes
  table_levels <- info$table_name[order(info$total_bytes)]
  info$table_label <- factor(info$table_name, levels = table_levels)

  files <- data.frame(
    table_name = rep(info$table_name, 2L),
    table_label = factor(rep(info$table_name, 2L), levels = table_levels),
    kind = factor(
      rep(c("data files", "delete files"), each = nrow(info)),
      levels = c("data files", "delete files")
    ),
    bytes = c(info$file_size_bytes, info$delete_file_size_bytes)
  )

  info$annotation <- ifelse(
    info$file_count == 0,
    "no data files",
    sprintf(
      "%d file%s, avg %s",
      info$file_count,
      ifelse(info$file_count == 1, "", "s"),
      format_bytes(info$file_size_bytes / pmax(info$file_count, 1))
    )
  )

  # Okabe-Ito hues (colorblind-safe); fixed name-to-color mapping
  file_colors <- c(
    "data files" = "#0072B2",
    "delete files" = "#D55E00"
  )

  subtitle <- sprintf(
    "%d table%s, %d data file%s, %s on disk",
    nrow(info),
    if (nrow(info) == 1) "" else "s",
    sum(info$file_count),
    if (sum(info$file_count) == 1) "" else "s",
    format_bytes(sum(info$total_bytes))
  )

  ggplot2::ggplot(
    files,
    ggplot2::aes(x = .data$bytes, y = .data$table_label, fill = .data$kind)
  ) +
    # Reversed stacking keeps data files anchored to the axis, with delete
    # files stacked outside them
    ggplot2::geom_col(width = 0.6, position = ggplot2::position_stack(reverse = TRUE)) +
    ggplot2::geom_text(
      data = info,
      ggplot2::aes(x = .data$total_bytes, y = .data$table_label, label = .data$annotation),
      inherit.aes = FALSE,
      hjust = -0.1, size = 3, color = "grey30"
    ) +
    # Right-side expansion leaves room for the annotation text
    ggplot2::scale_x_continuous(
      labels = format_bytes,
      expand = ggplot2::expansion(mult = c(0, 0.35))
    ) +
    ggplot2::scale_fill_manual(values = file_colors, drop = FALSE) +
    ggplot2::labs(
      title = sprintf("File layout of %s", ducklake_name),
      subtitle = subtitle,
      x = "Size on disk",
      y = "Table",
      fill = NULL
    ) +
    ggplot2::theme_minimal()
}

#' Format byte counts as human-readable sizes
#'
#' @noRd
format_bytes <- function(bytes) {
  vapply(bytes, function(b) {
    if (is.na(b)) {
      return(NA_character_)
    }
    units <- c("B", "kB", "MB", "GB", "TB")
    i <- 1
    while (b >= 1000 && i < length(units)) {
      b <- b / 1000
      i <- i + 1
    }
    if (i == 1) {
      sprintf("%.0f B", b)
    } else {
      sub("\\.0 ", " ", sprintf("%.1f %s", b, units[i]))
    }
  }, character(1))
}
