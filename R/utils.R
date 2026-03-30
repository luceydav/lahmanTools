#' Convert factor columns to character in a data.table
#'
#' Converts all factor columns in a `data.table` to character in-place (by
#' reference). Prevents DuckDB from inferring incompatible per-table ENUM
#' domains for the same column name, which causes cross-table join failures.
#'
#' @param dt A `data.table`. Modified by reference; no return value needed.
#'
#' @return `dt` invisibly.
#' @export
#'
#' @examples
#' library(data.table)
#' dt <- data.table(x = factor(c("a", "b")), y = 1:2)
#' dt_factors_to_char(dt)
#' class(dt$x)  # "character"
dt_factors_to_char <- function(dt) {
  factor_cols <- names(dt)[vapply(dt, is.factor, logical(1L))]
  if (length(factor_cols))
    dt[, (factor_cols) := lapply(.SD, as.character), .SDcols = factor_cols]
  invisible(dt)
}

#' Normalise column names to snake_case
#'
#' Converts strings like `"Average Annual"` or `"Total Value"` to lowercase
#' snake_case (`"average_annual"`, `"total_value"`), stripping trailing
#' underscores.  Used primarily to normalise scraped HTML field names before
#' writing to disk.
#'
#' @param x Character vector of names to normalise.
#'
#' @return Character vector the same length as `x`.
#' @export
#'
#' @examples
#' clean_names(c("Average Annual", "Total Value", "Player Name"))
#' # [1] "average_annual" "total_value"    "player_name"
clean_names <- function(x) {
  gsub("_+$", "", tolower(gsub("[^[:alnum:]]+", "_", x)))
}

#' Query a DuckDB connection and return a data.table
#'
#' Thin wrapper around [DBI::dbGetQuery()] that always returns a
#' `data.table` rather than a `data.frame`. Reduces session boilerplate when
#' running ad-hoc SQL against `baseball.duckdb`.
#'
#' @param con A `DBIConnection` object, typically from
#'   [connect_baseball_db()].
#' @param sql A single SQL string.
#' @param ... Additional arguments passed to [DBI::dbGetQuery()].
#'
#' @return A `data.table`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db()
#' db_query(con, "SELECT yearID, AVG(salary) FROM Salaries GROUP BY yearID")
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
db_query <- function(con, sql, ...) {
  data.table::as.data.table(DBI::dbGetQuery(con, sql, ...))
}

#' Normalise a player name for fuzzy matching
#'
#' Strips suffixes (Jr., Sr., II, III, IV), injury markers (*), accents,
#' punctuation in initials (J.D. -> J D), apostrophes, and extra whitespace.
#' Returns lowercase "last, first" form suitable for exact-match joining.
#'
#' @param x Character vector of player names in "Last, First" format.
#'
#' @return Character vector the same length as \code{x}, normalised.
#' @export
#'
#' @examples
#' normalise_player_name(c("Acuna Jr., Ronald", "Martinez, JD", "Harper, Bryce*"))
#' # [1] "acuna, ronald"  "martinez, j d"  "harper, bryce"
normalise_player_name <- function(x) {
  x <- gsub("\\*", "", x)                         # injury marker
  x <- gsub("\\b(Jr\\.?|Sr\\.?|II|III|IV)\\b", "", x)  # suffixes
  # Fix UTF-8 mojibake (e.g. Spotrac "CanÃ³" -> "Canó")
  x <- vapply(x, function(s) {
    if (!grepl("\u00c3", s, fixed = TRUE)) return(s)
    tryCatch({
      raw <- iconv(s, from = "UTF-8", to = "latin1", toRaw = TRUE)[[1L]]
      result <- rawToChar(raw)
      Encoding(result) <- "UTF-8"
      if (validUTF8(result)) result else s
    }, error = function(e) s)
  }, character(1L), USE.NAMES = FALSE)
  x <- iconv(x, from = "UTF-8", to = "ASCII//TRANSLIT") # accents
  x <- gsub("[\u2018\u2019\u0027]", "", x)        # straight + smart apostrophes
  # Expand bare initials: "Martinez, JD" -> "Martinez, J D" (only after comma)
  x <- gsub(",\\s*([A-Z])([A-Z])(?=[^a-z]|$)", ", \\1 \\2", x, perl = TRUE)
  x <- gsub("\\.", " ", x)                         # J.D. -> J D
  x <- gsub("[^[:alnum:], ]", "", x)              # other punctuation
  x <- gsub("\\s+", " ", trimws(x))               # collapse whitespace
  x <- gsub("\\s+,", ",", x)                      # space before comma
  x <- gsub(",\\s+", ", ", x)                      # normalise comma spacing
  tolower(x)
}

#' Match salary data to Lahman playerIDs via multi-pass name matching
#'
#' Performs progressive matching from strict to fuzzy: (1) exact "Last, First",
#' (2) normalised names (strips accents, suffixes, punctuation), (3) last-name
#' plus yearID-active filter for remaining ambiguous cases.
#'
#' @param sal_dt A `data.table` with a `player` column in "Last, First" format
#'   and a `yearID` column.
#' @param people_dt A `data.table` from `Lahman::People` with at least
#'   `playerID`, `nameFirst`, `nameLast`, `debut`, `finalGame`.
#'
#' @return \code{sal_dt} with a `playerID` column filled where matches succeed.
#'   Modified by reference; also returned invisibly.
#' @export
#'
#' @examples
#' \dontrun{
#' people <- data.table::as.data.table(Lahman::People)
#' sal <- data.table::fread("mlb_salaries/salaries_2023.csv")
#' match_player_ids(sal, people)
#' mean(!is.na(sal$playerID))  # match rate
#' }
match_player_ids <- function(sal_dt, people_dt) {
  stopifnot(
    data.table::is.data.table(sal_dt),
    data.table::is.data.table(people_dt),
    "player" %in% names(sal_dt),
    "yearID" %in% names(sal_dt),
    all(c("playerID", "nameFirst", "nameLast") %in% names(people_dt))
  )

  people <- data.table::copy(people_dt)

  # Build exact-match key: "Last, First"
  people[, player_exact := paste0(nameLast, ", ", nameFirst)]

  # Build normalised key
  people[, player_norm := normalise_player_name(player_exact)]

  # Derive active range from debut/finalGame (generous: +/- 1 year for edge cases)
  people[, debut_year := as.integer(substr(as.character(debut), 1L, 4L))]
  people[, final_year := as.integer(substr(as.character(finalGame), 1L, 4L))]
  # Active players have NA finalGame; set far future

  people[is.na(final_year), final_year := 2099L]
  people[is.na(debut_year), debut_year := 1800L]

  # Ensure playerID column exists in salary data
  if (!"playerID" %in% names(sal_dt)) sal_dt[, playerID := NA_character_]

  # --- Pass 1: Exact match on "Last, First" ---
  unmatched_idx <- which(is.na(sal_dt$playerID))
  if (length(unmatched_idx)) {
    exact_lookup <- people[, .(playerID, player_exact)]
    # Deduplicate: if multiple people share exact name, skip (ambiguous)
    exact_lookup <- exact_lookup[, .SD[.N == 1L], by = player_exact]
    m1 <- sal_dt[unmatched_idx, .(player, .row_idx = unmatched_idx)]
    m1 <- merge(m1, exact_lookup, by.x = "player", by.y = "player_exact",
                all.x = TRUE, sort = FALSE)
    matched <- !is.na(m1$playerID)
    if (any(matched)) {
      data.table::set(sal_dt, i = m1$.row_idx[matched], j = "playerID",
                      value = m1$playerID[matched])
    }
    msg_pass1 <- sum(matched)
  } else {
    msg_pass1 <- 0L
  }

  # --- Pass 2: Normalised name match ---
  unmatched_idx <- which(is.na(sal_dt$playerID))
  if (length(unmatched_idx)) {
    norm_lookup <- people[, .(playerID, player_norm)]
    norm_lookup <- norm_lookup[, .SD[.N == 1L], by = player_norm]
    m2 <- sal_dt[unmatched_idx, .(player, .row_idx = unmatched_idx)]
    m2[, player_norm := normalise_player_name(player)]
    m2 <- merge(m2, norm_lookup, by = "player_norm", all.x = TRUE, sort = FALSE)
    matched <- !is.na(m2$playerID)
    if (any(matched)) {
      data.table::set(sal_dt, i = m2$.row_idx[matched], j = "playerID",
                      value = m2$playerID[matched])
    }
    msg_pass2 <- sum(matched)
  } else {
    msg_pass2 <- 0L
  }

  # --- Pass 3: Normalised name + active-year disambiguation ---
  # For names that matched multiple people, use yearID to pick the right one
  unmatched_idx <- which(is.na(sal_dt$playerID))
  if (length(unmatched_idx)) {
    m3 <- sal_dt[unmatched_idx, .(player, yearID, .row_idx = unmatched_idx)]
    m3[, player_norm := normalise_player_name(player)]
    # Join to ALL normalised people (including ambiguous)
    all_norm <- people[, .(playerID, player_norm, debut_year, final_year)]
    m3_joined <- merge(m3, all_norm, by = "player_norm", all.x = TRUE,
                       allow.cartesian = TRUE, sort = FALSE)
    # Filter to active in that yearID
    m3_joined <- m3_joined[!is.na(playerID) &
                           yearID >= debut_year - 1L &
                           yearID <= final_year + 1L]
    # Keep only unambiguous (1 match per row)
    m3_joined[, n_matches := .N, by = .row_idx]
    m3_unique <- m3_joined[n_matches == 1L]
    if (nrow(m3_unique)) {
      data.table::set(sal_dt, i = m3_unique$.row_idx, j = "playerID",
                      value = m3_unique$playerID)
    }
    msg_pass3 <- nrow(m3_unique)
  } else {
    msg_pass3 <- 0L
  }

  total <- nrow(sal_dt)
  matched_total <- sum(!is.na(sal_dt$playerID))
  message(sprintf(
    "match_player_ids: %d/%d matched (%.1f%%). Pass1(exact)=%d, Pass2(normalised)=%d, Pass3(year-disambig)=%d",
    matched_total, total, 100 * matched_total / total,
    msg_pass1, msg_pass2, msg_pass3
  ))

  invisible(sal_dt)
}
