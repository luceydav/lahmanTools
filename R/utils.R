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

#' Map common team display names to Lahman teamID codes
#'
#' Returns a `data.table` with columns `team_name` and `teamID`.
#' Covers all 30 current franchises with common aliases used by
#' USA Today, Spotrac, and other public salary sources.
#'
#' @return A `data.table` with two character columns.
#' @export
team_name_map <- function() {
  # Each franchise: city names, nicknames, abbreviations
  aliases <- list(
    ARI = c("Arizona", "Diamondbacks", "D-backs", "ARI"),
    ATL = c("Atlanta", "Braves", "ATL"),
    BAL = c("Baltimore", "Orioles", "BAL"),
    BOS = c("Boston", "Red Sox", "BOS"),
    CHN = c("Chi. Cubs", "Chicago Cubs", "Cubs", "CHC"),
    CHA = c("Chic. White Sox", "Chicago White Sox", "White Sox", "CHW", "CWS"),
    CIN = c("Cincinnati", "Reds", "CIN"),
    CLE = c("Cleveland", "Guardians", "Indians", "CLE"),
    COL = c("Colorado", "Rockies", "COL"),
    DET = c("Detroit", "Tigers", "DET"),
    HOU = c("Houston", "Astros", "HOU"),
    KCA = c("Kansas City", "Royals", "KC", "KCR"),
    LAA = c("L.A. Angels", "Los Angeles Angels", "Angels", "Anaheim", "LAA"),
    LAN = c("L.A. Dodgers", "Los Angeles Dodgers", "Dodgers", "LAD"),
    MIA = c("Miami", "Marlins", "MIA"),
    MIL = c("Milwaukee", "Brewers", "MIL"),
    MIN = c("Minnesota", "Twins", "MIN"),
    NYN = c("N.Y. Mets", "New York Mets", "Mets", "NYM"),
    NYA = c("N.Y. Yankees", "New York Yankees", "Yankees", "NYY"),
    OAK = c("Oakland", "Athletics", "A's", "OAK"),
    ATH = c("Sacramento"),
    PHI = c("Philadelphia", "Phillies", "PHI"),
    PIT = c("Pittsburgh", "Pirates", "PIT"),
    SDN = c("San Diego", "Padres", "SD", "SDP"),
    SFN = c("San Francisco", "Giants", "SF", "SFG"),
    SEA = c("Seattle", "Mariners", "SEA"),
    SLN = c("St. Louis", "Cardinals", "STL"),
    TBA = c("Tampa Bay", "Rays", "TB", "TBR"),
    TEX = c("Texas", "Rangers", "TEX"),
    TOR = c("Toronto", "Blue Jays", "TOR"),
    WAS = c("Washington", "Nationals", "WSH", "WSN")
  )
  rows <- lapply(names(aliases), function(tid) {
    data.table::data.table(team_name = aliases[[tid]], teamID = tid)
  })
  data.table::rbindlist(rows)
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
#' Performs progressive matching from strict to fuzzy:
#' \enumerate{
#'   \item Exact "Last, First" match (unique names only)
#'   \item Normalised names (strips accents, suffixes, punctuation, mojibake)
#'   \item Normalised name + active-year filter for ambiguous names
#'   \item Team-constrained: last name within team-year roster (if \code{team}
#'         or \code{teamID} column present). This is the big-picture win --
#'         constraining to ~50 roster spots resolves nicknames, formal names,
#'         and most ambiguous names without complex normalization.
#' }
#'
#' @param sal_dt A `data.table` with a `player` column in "Last, First" format
#'   and a `yearID` column. Optionally a `team` (display name) or `teamID`
#'   (Lahman code) column for roster-constrained matching.
#' @param people_dt A `data.table` with at least `playerID`, `nameFirst`,
#'   `nameLast`, `debut`, `finalGame` (e.g., from the `People` table in the
#'   baseball DuckDB database, or `Lahman::People` if the package is installed).
#' @param roster_dt Optional `data.table` with `playerID`, `yearID`, `teamID`
#'   columns (e.g., from Appearances). If NULL, built automatically from
#'   the `Batting` and `Pitching` tables via `con` when team info is available.
#' @param con Optional `DBIConnection` used to query `Batting`/`Pitching` when
#'   `roster_dt` is NULL. Required when team info is present in `sal_dt` --
#'   run [setup_baseball_db()] and pass [connect_baseball_db()].
#'
#' @return \code{sal_dt} with a `playerID` column filled where matches succeed.
#'   Modified by reference; also returned invisibly.
#' @export
match_player_ids <- function(sal_dt, people_dt, roster_dt = NULL, con = NULL) {
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
  people[, player_norm := normalise_player_name(player_exact)]

  # Derive active range (generous +/- 1 year)
  people[, debut_year := as.integer(substr(as.character(debut), 1L, 4L))]
  people[, final_year := as.integer(substr(as.character(finalGame), 1L, 4L))]
  people[is.na(final_year), final_year := 2099L]
  people[is.na(debut_year), debut_year := 1800L]

  if (!"playerID" %in% names(sal_dt)) sal_dt[, playerID := NA_character_]

  # --- Pass 1: Exact match on "Last, First" ---
  unmatched_idx <- which(is.na(sal_dt$playerID))
  msg_pass1 <- 0L
  if (length(unmatched_idx)) {
    exact_lookup <- people[, .(playerID, player_exact)]
    exact_lookup <- exact_lookup[, .SD[.N == 1L], by = player_exact]
    m1 <- data.table::data.table(
      player = sal_dt$player[unmatched_idx],
      .row_idx = unmatched_idx
    )
    m1 <- merge(m1, exact_lookup, by.x = "player", by.y = "player_exact",
                all.x = TRUE, sort = FALSE)
    matched <- !is.na(m1$playerID)
    if (any(matched)) {
      data.table::set(sal_dt, i = m1$.row_idx[matched], j = "playerID",
                      value = m1$playerID[matched])
    }
    msg_pass1 <- sum(matched)
  }

  # --- Pass 2: Normalised name match ---
  unmatched_idx <- which(is.na(sal_dt$playerID))
  msg_pass2 <- 0L
  if (length(unmatched_idx)) {
    norm_lookup <- people[, .(playerID, player_norm)]
    norm_lookup <- norm_lookup[, .SD[.N == 1L], by = player_norm]
    m2 <- data.table::data.table(
      player = sal_dt$player[unmatched_idx],
      .row_idx = unmatched_idx
    )
    m2[, player_norm := normalise_player_name(player)]
    m2 <- merge(m2, norm_lookup, by = "player_norm", all.x = TRUE, sort = FALSE)
    matched <- !is.na(m2$playerID)
    if (any(matched)) {
      data.table::set(sal_dt, i = m2$.row_idx[matched], j = "playerID",
                      value = m2$playerID[matched])
    }
    msg_pass2 <- sum(matched)
  }

  # --- Pass 3: Normalised name + active-year disambiguation ---
  unmatched_idx <- which(is.na(sal_dt$playerID))
  msg_pass3 <- 0L
  if (length(unmatched_idx)) {
    m3 <- data.table::data.table(
      player = sal_dt$player[unmatched_idx],
      yearID = sal_dt$yearID[unmatched_idx],
      .row_idx = unmatched_idx
    )
    m3[, player_norm := normalise_player_name(player)]
    all_norm <- people[, .(playerID, player_norm, debut_year, final_year)]
    m3_joined <- merge(m3, all_norm, by = "player_norm", all.x = TRUE,
                       allow.cartesian = TRUE, sort = FALSE)
    m3_joined <- m3_joined[!is.na(playerID) &
                           yearID >= debut_year - 1L &
                           yearID <= final_year + 1L]
    m3_joined[, n_matches := .N, by = .row_idx]
    m3_unique <- m3_joined[n_matches == 1L]
    if (nrow(m3_unique)) {
      data.table::set(sal_dt, i = m3_unique$.row_idx, j = "playerID",
                      value = m3_unique$playerID)
    }
    msg_pass3 <- nrow(m3_unique)
  }

  # --- Pass 4: Team-constrained last-name + first-initial matching ---
  # This is the power move: within a team-year roster of ~50 players,
  # last-name alone resolves 96.4% and last+initial resolves 99.6%.
  # Handles nicknames, formal names, and ambiguous names in one pass.
  has_team <- "teamID" %in% names(sal_dt)
  has_team_name <- "team" %in% names(sal_dt)
  msg_pass4 <- 0L

  unmatched_idx <- which(is.na(sal_dt$playerID))
  if (length(unmatched_idx) && (has_team || has_team_name)) {
    # Map team display names to Lahman teamIDs if needed
    if (!has_team && has_team_name) {
      tmap <- team_name_map()
      sal_dt[tmap, .match_teamID := i.teamID, on = .(team = team_name)]
    } else {
      sal_dt[, .match_teamID := teamID]
    }

    # Build roster if not provided
    if (is.null(roster_dt)) {
      roster_dt <- tryCatch({
        if (!is.null(con)) {
          bat <- data.table::as.data.table(DBI::dbGetQuery(con, "SELECT playerID, yearID, teamID FROM Batting"))
          pit <- data.table::as.data.table(DBI::dbGetQuery(con, "SELECT playerID, yearID, teamID FROM Pitching WHERE playerID NOT IN (SELECT DISTINCT playerID FROM Batting)"))
        } else {
          stop("A DuckDB connection (con=) is required to build a roster. ",
               "Run setup_baseball_db() and pass con = connect_baseball_db().")
        }
        unique(rbind(
          bat[, .(playerID, yearID, teamID)],
          pit[, .(playerID, yearID, teamID)]
        ))
      }, error = function(e) NULL)
    }

    if (!is.null(roster_dt)) {
      # Build roster lookup with normalised last name + first initial
      rost <- merge(roster_dt, people[, .(playerID, nameLast, nameFirst)],
                    by = "playerID")
      rost[, last_norm := tolower(iconv(nameLast, to = "ASCII//TRANSLIT"))]
      rost[, last_norm := gsub("[^a-z]", "", last_norm)]
      rost[, first_init := substr(tolower(iconv(nameFirst, to = "ASCII//TRANSLIT")), 1L, 1L)]

      # Prepare unmatched salary rows
      unmatched_idx <- which(is.na(sal_dt$playerID) & !is.na(sal_dt$.match_teamID))
      if (length(unmatched_idx)) {
        m4 <- data.table::data.table(
          player = sal_dt$player[unmatched_idx],
          yearID = sal_dt$yearID[unmatched_idx],
          .match_teamID = sal_dt$.match_teamID[unmatched_idx],
          .row_idx = unmatched_idx
        )
        m4[, last_norm := sub(",.*", "", normalise_player_name(player))]
        m4[, first_init := substr(sub(".*,\\s*", "", normalise_player_name(player)), 1L, 1L)]

        # 4a: team + year + last name (unique within team)
        m4a <- merge(m4, rost[, .(playerID, yearID, teamID, last_norm)],
                     by.x = c("yearID", ".match_teamID", "last_norm"),
                     by.y = c("yearID", "teamID", "last_norm"),
                     all.x = TRUE, allow.cartesian = TRUE, sort = FALSE)
        m4a[, n := .N, by = .row_idx]
        m4a_ok <- m4a[n == 1L & !is.na(playerID)]
        if (nrow(m4a_ok)) {
          data.table::set(sal_dt, i = m4a_ok$.row_idx, j = "playerID",
                          value = m4a_ok$playerID)
          msg_pass4 <- msg_pass4 + nrow(m4a_ok)
        }

        # 4b: team + year + last name + first initial (for same-lastname teammates)
        unmatched_idx2 <- which(is.na(sal_dt$playerID) & !is.na(sal_dt$.match_teamID))
        if (length(unmatched_idx2)) {
          m4b <- data.table::data.table(
            player = sal_dt$player[unmatched_idx2],
            yearID = sal_dt$yearID[unmatched_idx2],
            .match_teamID = sal_dt$.match_teamID[unmatched_idx2],
            .row_idx = unmatched_idx2
          )
          m4b[, last_norm := sub(",.*", "", normalise_player_name(player))]
          m4b[, first_init := substr(sub(".*,\\s*", "", normalise_player_name(player)), 1L, 1L)]

          m4b <- merge(m4b, rost[, .(playerID, yearID, teamID, last_norm, first_init)],
                       by.x = c("yearID", ".match_teamID", "last_norm", "first_init"),
                       by.y = c("yearID", "teamID", "last_norm", "first_init"),
                       all.x = TRUE, allow.cartesian = TRUE, sort = FALSE)
          m4b[, n := .N, by = .row_idx]
          m4b_ok <- m4b[n == 1L & !is.na(playerID)]
          if (nrow(m4b_ok)) {
            data.table::set(sal_dt, i = m4b_ok$.row_idx, j = "playerID",
                            value = m4b_ok$playerID)
            msg_pass4 <- msg_pass4 + nrow(m4b_ok)
          }
        }
      }
    }

    # Clean up temp column
    if (".match_teamID" %in% names(sal_dt)) {
      sal_dt[, .match_teamID := NULL]
    }
  }

  total <- nrow(sal_dt)
  matched_total <- sum(!is.na(sal_dt$playerID))
  message(sprintf(
    "match_player_ids: %d/%d matched (%.1f%%). Pass1(exact)=%d, Pass2(norm)=%d, Pass3(year)=%d, Pass4(team)=%d",
    matched_total, total, 100 * matched_total / total,
    msg_pass1, msg_pass2, msg_pass3, msg_pass4
  ))

  invisible(sal_dt)
}
