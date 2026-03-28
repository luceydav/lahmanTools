library(rvest)
library(dplyr)
library(stringr)

years <- c(2017:2022)

for (year in years) {
  print(year)
  url <- paste0("https://www.spotrac.com/mlb/rankings/", year, "/salary/")
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
