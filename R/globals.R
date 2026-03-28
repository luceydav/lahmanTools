# Suppress R CMD check NOTEs for data.table's non-standard evaluation column
# references used throughout this package.
utils::globalVariables(c(
  # data.table specials
  ".",
  # scrape.R
  "salary", "average_annual", "player", "playerID", "yearID",
  # setup_db.R (also average_annual, playerID)
  # People columns used in scrape.R
  "nameLast", "nameFirst",
  # utils.R (dt_factors_to_char)
  "factor_cols"
))

#' @importFrom data.table := .SD as.data.table data.table fread fwrite rbindlist setnames
#' @importFrom utils data
NULL
