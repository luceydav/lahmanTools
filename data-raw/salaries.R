# LEGAL NOTICE ----------------------------------------------------------------
# This script scrapes salary data from Spotrac (spotrac.com).
# Spotrac's Terms of Service (https://www.spotrac.com/terms/) restrict
# automated access and commercial use of their data.
#
# This file is provided for REFERENCE ONLY. Before running it, you are
# responsible for:
#   1. Reviewing Spotrac's current Terms of Service
#   2. Determining whether your intended use is permitted
#   3. Obtaining any necessary permission from Spotrac
#
# The scraped data (CSV output) must NOT be committed to version control
# or redistributed. It is excluded via .gitignore.
#
# NOTE: This scraper uses an AJAX POST endpoint that returns all ~1500 players
# in a single request per year. The old CSS selectors (.player-name, .rank-position)
# are obsolete as of Spotrac's 2023 redesign.
# -----------------------------------------------------------------------------

library(rvest)
library(httr2)
library(data.table)

output_dir <- "mlb_salaries"
dir.create(output_dir, showWarnings = FALSE)

years <- 2017:2021   # USA Today covers 2022+; Spotrac fills the gap

# -- Parse one year's AJAX HTML -----------------------------------------------
parse_spotrac_table <- function(html, year) {
  rows <- html_elements(html, "li.list-group-item.d-flex")
  if (length(rows) == 0L) return(NULL)

  result <- data.table::rbindlist(lapply(rows, function(li) {
    name_node <- html_element(li, "div.link a")
    if (is.na(name_node)) return(NULL)

    player  <- html_text(name_node, trim = TRUE)
    sm_text <- html_text(html_element(li, "small"), trim = TRUE)
    # sm_text looks like "LAA, CF" — split on ", "
    tp      <- strsplit(sm_text, ",\\s*", perl = TRUE)[[1L]]
    team    <- if (length(tp) >= 1L) trimws(tp[[1L]]) else NA_character_
    pos     <- if (length(tp) >= 2L) trimws(tp[[2L]]) else NA_character_

    sal_raw <- html_text(html_element(li, "span.medium"), trim = TRUE)
    salary  <- suppressWarnings(
      as.numeric(gsub("[$,\\s]", "", sal_raw, perl = TRUE))
    )

    data.table::data.table(
      yearID   = as.integer(year),
      player   = player,
      team     = team,
      position = pos,
      salary   = salary
    )
  }), fill = TRUE)

  # Drop rows where salary parsed to NA (team-total summary rows at bottom)
  result[!is.na(salary) & !is.na(player)]
}

# -- Scrape year by year -------------------------------------------------------
for (year in years) {
  out_file <- file.path(output_dir, paste0("salaries_spotrac_", year, ".csv"))
  if (file.exists(out_file)) {
    message("Skipping ", year, " -- already saved.")
    next
  }

  message("Scraping Spotrac ", year, "...")
  Sys.sleep(3 + runif(1L, 0, 2))   # polite delay

  url <- paste0(
    "https://www.spotrac.com/mlb/rankings/player/_/year/", year, "/sort/cash_total"
  )

  resp <- tryCatch(
    request(url) |>
      req_method("POST") |>
      req_body_raw("ajax=table", "application/x-www-form-urlencoded") |>
      req_headers(`User-Agent` = "Mozilla/5.0") |>
      req_perform(),
    error = function(e) { message("  ERROR: ", conditionMessage(e)); NULL }
  )

  if (is.null(resp) || resp_status(resp) != 200L) {
    warning("No response for year ", year)
    next
  }

  html    <- resp_body_html(resp)
  yr_data <- parse_spotrac_table(html, year)

  if (!is.null(yr_data) && nrow(yr_data) > 0L) {
    data.table::fwrite(yr_data, out_file)
    message(sprintf("  Saved %d players for %s -> %s", nrow(yr_data), year, out_file))
  } else {
    warning("No data parsed for year ", year)
  }
}

# -- Combine and join to Lahman playerID --------------------------------------
spotrac_files <- list.files(output_dir,
                             pattern = "salaries_spotrac_\\d{4}\\.csv",
                             full.names = TRUE)

if (length(spotrac_files) == 0L) stop("No Spotrac CSV files found in ", output_dir)

all_sal <- data.table::rbindlist(lapply(spotrac_files, data.table::fread), fill = TRUE)

# Reformat player column to "Last, First" to match Lahman People format
# Spotrac stores as "First Last" -- reverse the order
# Strip suffixes (Jr., Sr., II, III) BEFORE reversing to avoid "Jr., Jackie Bradley"
suffix_pat <- "\\s+(Jr\\.?|Sr\\.?|II|III|IV)$"
all_sal[, player := gsub(suffix_pat, "", player)]
name_parts <- strsplit(all_sal$player, "\\s+", perl = TRUE)
all_sal[, player := vapply(name_parts, function(p) {
  if (length(p) < 2L) return(p[[1L]])
  paste0(p[[length(p)]], ", ", paste(p[-length(p)], collapse = " "))
}, character(1L))]

people <- data.table::as.data.table(Lahman::People)
match_player_ids(all_sal, people)

match_pct <- mean(!is.na(all_sal$playerID)) * 100
message(sprintf("Final match rate: %.1f%% of %d rows", match_pct, nrow(all_sal)))

yr_range    <- range(all_sal$yearID, na.rm = TRUE)
out_combined <- file.path(
  output_dir,
  sprintf("salaries_spotrac_%d_%d_with_playerID.csv", yr_range[[1L]], yr_range[[2L]])
)
data.table::fwrite(all_sal, out_combined)
data.table::fwrite(
  unique(all_sal[is.na(playerID), .(player)]),
  file.path(output_dir, "unmatched_spotrac.csv")
)

message("Done. Combined file: ", out_combined)
