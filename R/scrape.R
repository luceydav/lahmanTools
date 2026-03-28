#' Scrape MLB salary data from USA Today
#'
#' Fetches individual player salary pages from `databases.usatoday.com`,
#' saves one CSV per year to `output_dir`, then combines them and joins to
#' Lahman `playerID` via last-name/first-name matching.
#'
#' Year slugs follow two URL patterns:
#' - 2017-2022: `mlb-salaries-{year}`
#' - 2023+: `major-league-baseball-salaries-{year}`
#'
#' Already-completed year files are skipped automatically.
#'
#' @param years Integer vector of seasons to scrape. Defaults to all known
#'   seasons (2017-2025).
#' @param output_dir Directory for per-year CSV output and the combined file.
#'
#' @return Invisibly returns the path to the combined
#'   `salaries_<min>_<max>_with_playerID.csv` file.
#' @export
#'
#' @examples
#' \dontrun{
#' scrape_salaries(years = 2025)
#' }
scrape_salaries <- function(years      = 2017:2025,
                             output_dir = "mlb_salaries") {
  year_slugs <- c(
    "2017" = "mlb-salaries-2017",
    "2018" = "mlb-salaries-2018",
    "2019" = "mlb-salaries-2019",
    "2020" = "mlb-salaries-2020",
    "2021" = "mlb-salaries-2021",
    "2022" = "mlb-salaries-2022",
    "2023" = "major-league-baseball-salaries-2023",
    "2024" = "major-league-baseball-salaries-2024",
    "2025" = "major-league-baseball-salaries-2025"
  )

  years <- as.character(years)
  missing_years <- setdiff(years, names(year_slugs))
  if (length(missing_years)) {
    stop("No URL slug defined for year(s): ", paste(missing_years, collapse = ", "),
         "\nAdd entries to year_slugs inside scrape_salaries().")
  }
  year_slugs <- year_slugs[years]

  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  # -- Per-year scrape ----------------------------------------------------------
  for (yr in names(year_slugs)) {
    out_file <- file.path(output_dir, paste0("salaries_", yr, ".csv"))
    if (file.exists(out_file)) {
      message("Skipping ", yr, " -- already saved.")
      next
    }

    slug <- year_slugs[[yr]]
    message("Scraping ", yr, "...")

    yr_data <- data.table::rbindlist(
      lapply(seq_len(1000), function(id) {
        if (id %% 100 == 0) message("  ... record ", id)
        scrape_player_(slug, yr, id)
      }),
      fill = TRUE
    )

    if (nrow(yr_data) > 0) {
      data.table::fwrite(yr_data, out_file)
      message(sprintf("  Saved %d players for %s -> %s", nrow(yr_data), yr, out_file))
    } else {
      warning("No data retrieved for ", yr)
    }
  }

  # -- Combine all years --------------------------------------------------------
  message("Combining all years...")
  all_files <- list.files(output_dir, pattern = "salaries_\\d{4}\\.csv",
                          full.names = TRUE)
  all_salaries <- data.table::rbindlist(lapply(all_files, data.table::fread),
                                        fill = TRUE)
  all_salaries[, salary := as.numeric(gsub("[$,]", "", salary))]

  # -- Join to Lahman playerID --------------------------------------------------
  people <- data.table::as.data.table(Lahman::People)
  people[, player := paste0(nameLast, ", ", nameFirst)]

  sal_linked <- merge(all_salaries, people[, .(playerID, player)],
                      by = "player", all.x = TRUE)

  match_pct <- mean(!is.na(sal_linked$playerID)) * 100
  message(sprintf("Matched: %.1f%% of %d rows", match_pct, nrow(sal_linked)))

  yr_range    <- range(sal_linked$yearID, na.rm = TRUE)
  out_combined <- file.path(
    output_dir,
    sprintf("salaries_%d_%d_with_playerID.csv", yr_range[1], yr_range[2])
  )
  data.table::fwrite(sal_linked, out_combined)
  data.table::fwrite(unique(sal_linked[is.na(playerID), .(player)]),
                     file.path(output_dir, "unmatched_players.csv"))

  message("Done. Combined file: ", out_combined)
  invisible(out_combined)
}

# -- Internal: scrape one player page -----------------------------------------
scrape_player_ <- function(slug, year, id) {
  url <- paste0("https://databases.usatoday.com/", slug, "/", id, "/")

  resp <- tryCatch(
    httr2::request(url) |>
      httr2::req_headers(`User-Agent` = "Mozilla/5.0") |>
      httr2::req_throttle(5) |>
      httr2::req_perform(),
    error = function(e) NULL
  )

  if (is.null(resp) || httr2::resp_status(resp) %in% c(403L, 404L)) return(NULL)

  page  <- httr2::resp_body_html(resp)
  nodes <- rvest::html_elements(page, "h4")

  core_fields <- c("Player", "Team", "Position", "Salary", "Years",
                   "Total Value", "Average Annual")

  pairs <- data.table::rbindlist(lapply(nodes, function(node) {
    label <- rvest::html_text(node, trim = TRUE)
    if (!label %in% core_fields) return(NULL)
    value <- tryCatch(
      xml2::xml_find_first(node, "following-sibling::*[1]") |>
        rvest::html_text(trim = TRUE),
      error = function(e) NA_character_
    )
    data.table::data.table(field = label, value = value)
  }))

  if (nrow(pairs) == 0 || !("Player" %in% pairs$field)) return(NULL)

  pairs <- unique(pairs, by = "field")
  result <- data.table::as.data.table(as.list(stats::setNames(pairs$value, pairs$field)))

  clean_nm <- clean_names
  data.table::setnames(result, clean_nm(names(result)))

  result[, `:=`(yearID = as.integer(year), url_id = id)]
  result
}
