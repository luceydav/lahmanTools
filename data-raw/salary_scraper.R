library(rvest)
library(httr2)
library(data.table)
library(Lahman)

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

year_slugs <- year_slugs[9]

output_dir <- "mlb_salaries"
dir.create(output_dir, showWarnings = FALSE)

scrape_player <- function(slug, year, id) {
  url <- paste0("https://databases.usatoday.com/", slug, "/", id, "/")
  
  resp <- tryCatch(
    request(url) |>
      req_headers(`User-Agent` = "Mozilla/5.0") |>
      req_throttle(5) |>
      req_perform(),
    error = function(e) NULL
  )
  
  if (is.null(resp) || resp_status(resp) %in% c(404, 403)) return(NULL)
  
  page  <- resp |> resp_body_html()
  nodes <- page |> html_elements("h4")
  
  core_fields <- c("Player", "Team", "Position", "Salary", "Years",
                   "Total Value", "Average Annual")
  
  pairs <- rbindlist(lapply(nodes, function(node) {
    label <- html_text(node, trim = TRUE)
    if (!label %in% core_fields) return(NULL)
    value <- tryCatch(
      node |> xml2::xml_find_first("following-sibling::*[1]") |> html_text(trim = TRUE),
      error = function(e) NA_character_
    )
    data.table(field = label, value = value)
  }))
  
  if (nrow(pairs) == 0 || !("Player" %in% pairs$field)) return(NULL)
  
  # pivot to one-row wide table; keep first occurrence if field repeats
  pairs <- unique(pairs, by = "field")
  result <- as.data.table(as.list(setNames(pairs$value, pairs$field)))
  
  # clean names: lowercase, non-alphanumeric runs -> underscore, trim trailing _
  clean <- function(x) gsub("_+$", "", tolower(gsub("[^[:alnum:]]+", "_", x)))
  setnames(result, clean(names(result)))
  
  result[, `:=`(yearID = as.integer(year), url_id = id)]
  result
}

# ── Scrape year by year, skipping already-completed years ─────────────────────
for (yr in names(year_slugs)) {
  out_file <- file.path(output_dir, paste0("salaries_", yr, ".csv"))
  
  # Skip if already done
  if (file.exists(out_file)) {
    message("Skipping ", yr, " — already saved.")
    next
  }
  
  slug <- year_slugs[[yr]]
  message("Scraping ", yr, "...")
  
  yr_data <- rbindlist(lapply(seq_len(1000), function(id) {
    if (id %% 100 == 0) message("  ... record ", id)
    scrape_player(slug, yr, id)
  }), fill = TRUE)
  
  if (nrow(yr_data) > 0) {
    fwrite(yr_data, out_file)
    message("Saved ", nrow(yr_data), " players for ", yr, " -> ", out_file)
  } else {
    message("WARNING: No data retrieved for ", yr)
  }
}

# ── Combine all years ─────────────────────────────────────────────────────────
message("Combining all years...")

all_salaries <- rbindlist(
  lapply(
    list.files(output_dir, pattern = "salaries_\\d{4}\\.csv", full.names = TRUE),
    fread
  ),
  fill = TRUE
)
all_salaries[, salary := as.numeric(gsub("[$,]", "", salary))]

# ── Join to Lahman playerID ───────────────────────────────────────────────────
people <- as.data.table(Lahman::People)
people[, player := paste0(nameLast, ", ", nameFirst)]

sal_linked <- merge(all_salaries, people[, .(playerID, player)],
                    by = "player", all.x = TRUE)

message(sprintf("Matched: %.1f%% of %d rows",
                mean(!is.na(sal_linked$playerID)) * 100, nrow(sal_linked)))

fwrite(sal_linked,
       file.path(output_dir, "salaries_2017_2024_with_playerID.csv"))

fwrite(unique(sal_linked[is.na(playerID), .(player)]),
       file.path(output_dir, "unmatched_players.csv"))

message("Done. Final file: ", file.path(output_dir, "salaries_2017_2024_with_playerID.csv"))