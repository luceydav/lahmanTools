# ── Internal view helpers (called by loaders; testable without network) ────────

create_player_ids_view_ <- function(con) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW PlayerIDs AS
    SELECT
      p.playerID,
      p.bbrefID,
      p.retroID,
      c.key_mlbam    AS mlbam_id,
      c.key_fangraphs AS fg_id,
      c.key_npb      AS npb_id,
      p.nameFirst,
      p.nameLast,
      p.birthYear,
      p.debut
    FROM People p
    LEFT JOIN ChadwickIDs c
      ON p.bbrefID = c.key_bbref
  ")
  message(sprintf("  %-25s (view)", "PlayerIDs"))
  invisible(con)
}

create_war_views_ <- function(con) {
  # PlayerWAR: unified batting + pitching fWAR joined to Lahman playerID via
  # Chadwick.  FanGraphs player IDs are stored as VARCHAR in both tables.
  # Two-player seasons (traded players) are collapsed to one row per yearID by
  # summing WAR components; salary joins happen at the same grain.
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW PlayerWAR AS
    WITH bat AS (
      SELECT
        p.playerID,
        fw.Season::INTEGER  AS yearID,
        fw.WAR::DOUBLE      AS bat_war
      FROM FangraphsBattingWAR fw
      JOIN ChadwickIDs c ON fw.playerid::VARCHAR = c.key_fangraphs::VARCHAR
      JOIN People p ON c.key_bbref = p.bbrefID
      WHERE fw.WAR IS NOT NULL
    ),
    pit AS (
      SELECT
        p.playerID,
        fw.Season::INTEGER  AS yearID,
        fw.WAR::DOUBLE      AS pit_war
      FROM FangraphsPitchingWAR fw
      JOIN ChadwickIDs c ON fw.playerid::VARCHAR = c.key_fangraphs::VARCHAR
      JOIN People p ON c.key_bbref = p.bbrefID
      WHERE fw.WAR IS NOT NULL
    )
    SELECT
      COALESCE(b.playerID,  pt.playerID)  AS playerID,
      COALESCE(b.yearID,    pt.yearID)    AS yearID,
      COALESCE(b.bat_war,  0.0)           AS bat_war,
      COALESCE(pt.pit_war, 0.0)           AS pit_war,
      COALESCE(b.bat_war,  0.0)
        + COALESCE(pt.pit_war, 0.0)       AS total_war
    FROM bat b
    FULL OUTER JOIN pit pt USING (playerID, yearID)
  ")
  message(sprintf("  %-25s (view)", "PlayerWAR"))

  # SalaryPerWAR: dollars per WAR by player-season.
  #
  # war_reliable flag:
  #   FanGraphs pitching WAR is only available from 2002 onward.  A pitcher
  #   with salary data before 2002 will have near-zero total_war (batting
  #   contribution only), making dollars_per_war badly wrong for that row.
  #   war_reliable = FALSE when the player had pitching appearances AND
  #   yearID < 2002.  Filter WHERE war_reliable = TRUE for clean analysis.
  #   Batting WAR is reliable for all seasons 1985+.
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW SalaryPerWAR AS
    WITH pitcher_seasons AS (
      SELECT DISTINCT playerID, yearID
      FROM Pitching
      WHERE G > 0
    )
    SELECT
      s.playerID,
      s.yearID,
      s.teamID,
      s.salary,
      s.source                           AS salary_source,
      w.bat_war,
      w.pit_war,
      w.total_war,
      s.salary / NULLIF(w.total_war, 0)  AS dollars_per_war,
      era_label(s.yearID)                AS era,
      NOT (ps.playerID IS NOT NULL AND s.yearID < 2002) AS war_reliable
    FROM SalariesAll s
    JOIN PlayerWAR w USING (playerID, yearID)
    LEFT JOIN pitcher_seasons ps USING (playerID, yearID)
    WHERE s.is_actual = TRUE
      AND s.salary    > 0
      AND w.total_war > 0
  ")
  message(sprintf("  %-25s (view)", "SalaryPerWAR"))
  invisible(con)
}

create_statcast_season_view_ <- function(con) {
  # Batter-season aggregates from pitch-level Statcast data.
  # Join to PlayerIDs.mlbam_id to get Lahman playerID.
  # Filters to batted balls for exit-velocity metrics; PA count uses events
  # column (non-NULL events mark plate appearance terminations).
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW StatcastSeason AS
    SELECT
      batter::VARCHAR                   AS mlbam_id,
      game_year::INTEGER                AS yearID,
      COUNT(*)                          AS pitches_seen,
      COUNT(*) FILTER (
        WHERE events IS NOT NULL
          AND events NOT IN ('', 'null')
      )                                 AS pa,
      AVG(launch_speed) FILTER (
        WHERE launch_speed IS NOT NULL
      )                                 AS avg_exit_velo,
      MAX(launch_speed) FILTER (
        WHERE launch_speed IS NOT NULL
      )                                 AS max_exit_velo,
      -- Hard-hit rate: batted balls with exit velo >= 95 mph
      AVG(CASE WHEN launch_speed >= 95 THEN 1.0 ELSE 0.0 END)
        FILTER (WHERE launch_speed IS NOT NULL)
                                        AS hard_hit_pct,
      AVG(launch_angle) FILTER (
        WHERE launch_angle IS NOT NULL
      )                                 AS avg_launch_angle,
      AVG(estimated_ba_using_speedangle)  AS xBA,
      AVG(estimated_woba_using_speedangle) AS xwOBA
    FROM StatcastPitches
    GROUP BY batter, game_year
  ")
  message(sprintf("  %-25s (view)", "StatcastSeason"))
  invisible(con)
}


# ── Public loaders ─────────────────────────────────────────────────────────────

#' Load Chadwick Bureau player ID crosswalk
#'
#' Downloads the Chadwick Bureau persons register via \pkg{baseballr} and
#' writes it to a `ChadwickIDs` table in `con`.  Creates a `PlayerIDs` view
#' that joins Chadwick IDs to the Lahman `People` table so every player has
#' MLB Advanced Media (MLBAM), FanGraphs, Retrosheet and Baseball Reference IDs
#' alongside their Lahman `playerID`.
#'
#' **Attribution:** Chadwick Baseball Bureau persons register,
#' <https://github.com/chadwickbureau/register>,
#' licensed under the Open Data Commons Attribution License (ODC-BY 1.0).
#'
#' @param con A writable `DBIConnection` to the baseball DuckDB database.
#' @param overwrite Logical.  Drop and recreate the table if it already
#'   exists.  Default `FALSE` leaves an existing table untouched.
#'
#' @return Invisibly returns `con`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db(read_only = FALSE)
#' load_chadwick_ids(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
load_chadwick_ids <- function(con, overwrite = FALSE) {
  if (!requireNamespace("baseballr", quietly = TRUE))
    stop("Package 'baseballr' is required. Install with: install.packages('baseballr')")

  message("Downloading Chadwick Bureau player register...")
  register <- data.table::as.data.table(baseballr::chadwick_player_lu())

  keep_cols <- intersect(
    c("key_person", "key_mlbam", "key_retro", "key_bbref",
      "key_fangraphs", "key_npb", "name_last", "name_first"),
    names(register)
  )
  register <- register[, ..keep_cols]

  # Normalise to VARCHAR -- FanGraphs IDs are integers in some baseballr builds
  if ("key_fangraphs" %in% names(register))
    register[, key_fangraphs := as.character(key_fangraphs)]
  if ("key_mlbam" %in% names(register))
    register[, key_mlbam := as.character(key_mlbam)]

  DBI::dbWriteTable(con, "ChadwickIDs", register, overwrite = overwrite)
  message(sprintf("  %-25s %d rows", "ChadwickIDs", nrow(register)))

  create_player_ids_view_(con)
  invisible(con)
}


#' Load FanGraphs WAR data
#'
#' Fetches Wins Above Replacement leaderboard data from FanGraphs via
#' \pkg{baseballr} for the requested seasons, writes batters to
#' `FangraphsBattingWAR` and pitchers to `FangraphsPitchingWAR`, then creates
#' two derived views:
#'
#' - **`PlayerWAR`** -- one row per player-season with `bat_war`, `pit_war`,
#'   `total_war`, joined to Lahman `playerID` via Chadwick.
#' - **`SalaryPerWAR`** -- joins `PlayerWAR` to `SalariesAll`; reports
#'   `salary`, `total_war`, `dollars_per_war`, and `era_label`.
#'
#' **Prerequisites:** [load_chadwick_ids()] must be run first (the join to
#' Lahman `playerID` routes through `ChadwickIDs`).
#'
#' **Data note:** FanGraphs data is copyright FanGraphs.  This function
#' performs a runtime fetch to your local database only.  Do not redistribute
#' the fetched data.
#'
#' @param con A writable `DBIConnection` to the baseball DuckDB database.
#' @param years Integer vector of seasons to fetch.  Defaults to `1985:2025`
#'   (aligns with Lahman `Salaries` coverage).
#' @param overwrite Logical.  Drop and recreate existing tables.  Default
#'   `FALSE`.
#'
#' @return Invisibly returns `con`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db(read_only = FALSE)
#' load_chadwick_ids(con)
#' load_fangraphs_war(con, years = 2010:2025)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
load_fangraphs_war <- function(con, years = 1985:2025, overwrite = FALSE) {
  if (!requireNamespace("baseballr", quietly = TRUE))
    stop("Package 'baseballr' is required. Install with: install.packages('baseballr')")
  if (!("ChadwickIDs" %in% DBI::dbListTables(con)))
    stop("ChadwickIDs table not found. Run load_chadwick_ids(con) first.")

  start_yr <- min(years)
  end_yr   <- max(years)

  message(sprintf("Fetching FanGraphs batting WAR %d-%d...", start_yr, end_yr))
  bat <- tryCatch(
    data.table::as.data.table(
      baseballr::fg_batter_leaders(
        startseason = start_yr, endseason = end_yr, qual = 0, ind = 1
      )
    ),
    error = function(e)
      stop("Failed to fetch FanGraphs batting WAR: ", conditionMessage(e))
  )

  message(sprintf("Fetching FanGraphs pitching WAR %d-%d...", start_yr, end_yr))
  pit <- tryCatch(
    data.table::as.data.table(
      baseballr::fg_pitcher_leaders(
        startseason = start_yr, endseason = end_yr, qual = 0, ind = 1
      )
    ),
    error = function(e)
      stop("Failed to fetch FanGraphs pitching WAR: ", conditionMessage(e))
  )

  # Normalise FG player ID to VARCHAR before writing
  if ("playerid" %in% names(bat)) bat[, playerid := as.character(playerid)]
  if ("playerid" %in% names(pit)) pit[, playerid := as.character(playerid)]

  DBI::dbWriteTable(con, "FangraphsBattingWAR",  bat, overwrite = overwrite)
  DBI::dbWriteTable(con, "FangraphsPitchingWAR", pit, overwrite = overwrite)
  message(sprintf("  %-25s %d rows", "FangraphsBattingWAR",  nrow(bat)))
  message(sprintf("  %-25s %d rows", "FangraphsPitchingWAR", nrow(pit)))

  create_war_views_(con)
  invisible(con)
}


#' Load Statcast pitch-level data
#'
#' Fetches Baseball Savant pitch-level data via \pkg{baseballr} for each
#' requested season, appends to a `StatcastPitches` table, and creates a
#' `StatcastSeason` view with batter-season aggregates (exit velocity, launch
#' angle, hard-hit rate, xBA, xwOBA).
#'
#' `StatcastSeason.mlbam_id` maps to `PlayerIDs.mlbam_id` -- join those two
#' views to attach Lahman `playerID` and enable cross-dataset analysis.
#'
#' **Data note:** Statcast data is copyright MLB Advanced Media (MLBAM).
#' This function performs a runtime fetch to your local database only.
#' Do not redistribute the fetched data.
#'
#' Pitch-level data is large -- roughly 700 MB per season uncompressed.
#' Load one year at a time and allow DuckDB to handle compression on disk.
#' Statcast data is only available from 2015 onward.
#'
#' @param con A writable `DBIConnection` to the baseball DuckDB database.
#' @param years Integer vector of seasons to fetch (2015 or later required).
#' @param game_type One of `"R"` (regular season, default), `"P"`
#'   (postseason), or `"S"` (spring training).
#' @param overwrite Logical.  If `TRUE`, drop and recreate
#'   `StatcastPitches` before loading the first year.  If `FALSE` (default),
#'   append new seasons to any existing data.
#'
#' @return Invisibly returns `con`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db(read_only = FALSE)
#' load_statcast(con, years = 2023)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
load_statcast <- function(con, years, game_type = "R", overwrite = FALSE) {
  if (!requireNamespace("baseballr", quietly = TRUE))
    stop("Package 'baseballr' is required. Install with: install.packages('baseballr')")

  years <- as.integer(years)
  if (any(years < 2015L))
    stop("Statcast data is only available from 2015 onward.")

  message(sprintf(
    "Loading Statcast data for %d season(s). Expect ~700 MB / season.",
    length(years)
  ))

  first_write <- overwrite
  for (yr in sort(years)) {
    message(sprintf("  Fetching Statcast %d...", yr))
    start_dt <- sprintf("%d-03-01", yr)
    end_dt   <- sprintf("%d-12-01", yr)

    sc <- tryCatch(
      data.table::as.data.table(
        baseballr::statcast_search(
          start_date  = start_dt,
          end_date    = end_dt,
          player_type = "batter"
        )
      ),
      error = function(e) {
        warning(sprintf("Failed to fetch Statcast %d: %s", yr, conditionMessage(e)))
        NULL
      }
    )

    if (is.null(sc) || nrow(sc) == 0L) {
      warning(sprintf("No Statcast data returned for %d -- skipping.", yr))
      next
    }

    DBI::dbWriteTable(con, "StatcastPitches", sc,
                      overwrite = first_write, append = !first_write)
    message(sprintf("    Wrote %d rows for %d", nrow(sc), yr))
    first_write <- FALSE
  }

  create_statcast_season_view_(con)
  invisible(con)
}
