# Suppress R CMD check NOTEs for data.table's non-standard evaluation column
# references used throughout this package.
utils::globalVariables(c(
  # data.table specials
  ".",
  # scrape.R
  "salary", "average_annual", "player", "playerID", "yearID",
  # setup_db.R (also average_annual, playerID)
  # People columns used in scrape.R / match_player_ids
  "nameLast", "nameFirst",
  # match_player_ids internal columns
  "player_exact", "player_norm", "debut_year", "final_year",
  ".row_idx", "n_matches", ".match_teamID", "last_norm", "first_init", "n",
  # utils.R (dt_factors_to_char)
  "factor_cols",
  # loaders.R -- Chadwick register column references
  "key_fangraphs", "key_mlbam",
  # loaders.R -- FanGraphs leaderboard column
  "playerid"
))

#' @importFrom data.table := .SD as.data.table data.table fread fwrite rbindlist setnames
#' @importFrom utils data
NULL
