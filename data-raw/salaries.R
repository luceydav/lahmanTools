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
# -----------------------------------------------------------------------------

library(rvest)
library(data.table)
library(stringr)

years <- c(2017:2022)

for (year in years) {
  print(year)
  url <- paste0("https://www.spotrac.com/mlb/rankings/", year, "/salary/")
  Sys.sleep(3 + runif(1, 0, 2))   # ~3-5 second polite delay between years
  html <- read_html(url)

  players <- html %>%
    html_nodes(".player-name") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  positions <- html %>%
    html_nodes(".rank-position") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  ages <- html %>%
    html_nodes(".center:nth-child(4)") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  bats <- html %>%
    html_nodes(".center:nth-child(5)") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  throws <- html %>%
    html_nodes(".center:nth-child(6)") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  salaries <- html %>%
    html_nodes(".right") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  salaries_table <-
    data.frame(
      year = year,
      player = players,
      position = positions,
      age = ages,
      bats = bats,
      throws = throws,
      salary = salaries
    )
  if (length(players) == length(positions) &&
      length(positions) == length(ages) &&
      length(ages) == length(bats) &&
      length(bats) == length(throws) &&
      length(throws) == length(salaries)) {
    salaries_table <-
      data.frame(
        year = year,
        player = players,
        position = positions,
        age = ages,
        bats = bats,
        throws = throws,
        salary = salaries
      )
    salary_list <-
      append(salary_list, list(salaries_table))
  }
}
