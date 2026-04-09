# ── Internal view helpers (called by loaders; testable without network) ────────

create_player_ids_view_ <- function(con) {
  if (!("People" %in% DBI::dbListTables(con))) {
    message("  PlayerIDs view skipped -- People table not present (run setup_baseball_db first).")
    return(invisible(con))
  }
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
  tbls <- DBI::dbListTables(con)
  needed <- c("People", "ChadwickIDs", "FangraphsBattingWAR",
              "FangraphsPitchingWAR", "SalariesAll", "Pitching")
  missing <- setdiff(needed, tbls)
  if (length(missing)) {
    message("  WAR views skipped -- missing tables: ", paste(missing, collapse = ", "))
    return(invisible(con))
  }
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
  # war_reliable flag: kept for backward compatibility; now always TRUE
  # since FanGraphs pitching WAR covers the full salary era (1985+).
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
      NOT (ps.playerID IS NOT NULL AND s.yearID < 1985) AS war_reliable
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
  bat_list <- lapply(years, function(yr) {
    tryCatch({
      d <- data.table::as.data.table(
        baseballr::fg_bat_leaders(startseason = yr, endseason = yr, qual = 0)
      )
      if ("playerid" %in% names(d)) d[, playerid := as.character(playerid)]
      d
    }, error = function(e) {
      warning(sprintf("FanGraphs batting WAR unavailable for %d: %s", yr, conditionMessage(e)))
      NULL
    })
  })
  bat <- data.table::rbindlist(Filter(Negate(is.null), bat_list), fill = TRUE)

  message(sprintf("Fetching FanGraphs pitching WAR %d-%d...", start_yr, end_yr))
  pit_list <- lapply(years, function(yr) {
    tryCatch({
      d <- data.table::as.data.table(
        baseballr::fg_pitch_leaders(startseason = yr, endseason = yr, qual = 0)
      )
      if ("playerid" %in% names(d)) d[, playerid := as.character(playerid)]
      d
    }, error = function(e) {
      warning(sprintf("FanGraphs pitching WAR unavailable for %d: %s", yr, conditionMessage(e)))
      NULL
    })
  })
  pit <- data.table::rbindlist(Filter(Negate(is.null), pit_list), fill = TRUE)

  if (nrow(bat) == 0L) stop("No FanGraphs batting WAR data retrieved.")
  if (nrow(pit) == 0L) warning("No FanGraphs pitching WAR data retrieved.")

  DBI::dbWriteTable(con, "FangraphsBattingWAR",  bat, overwrite = overwrite)
  DBI::dbWriteTable(con, "FangraphsPitchingWAR", pit, overwrite = overwrite)
  message(sprintf("  %-25s %d rows", "FangraphsBattingWAR",  nrow(bat)))
  message(sprintf("  %-25s %d rows", "FangraphsPitchingWAR", nrow(pit)))

  create_war_views_(con)
  invisible(con)
}


#' Load Retrosheet postseason data (2022+)
#'
#' Downloads Retrosheet simplified CSV files and appends postseason player
#' statistics for the requested seasons to the `BattingPost`, `PitchingPost`,
#' and `SeriesPost` tables in the database.
#'
#' The Lahman `BattingPost`, `PitchingPost`, and `SeriesPost` tables stop at
#' 2021.  This function extends them using Retrosheet data (available through
#' 2025) by:
#'
#' \enumerate{
#'   \item Downloading the Retrosheet simplified CSV archive
#'     (`basiccsvs.zip`) to a local path.
#'   \item Filtering to the requested \code{years} and postseason game types
#'     (wildcard, divisionseries, lcs, worldseries).
#'   \item Mapping Retrosheet player IDs to Lahman \code{playerID} via
#'     \code{People$retroID}.
#'   \item Mapping Retrosheet team codes to Lahman \code{teamID} and
#'     \code{lgID} via the \code{Teams} table.
#'   \item Deriving Lahman-style \code{round} codes (e.g. \code{ALCS},
#'     \code{ALDS1}, \code{ALWC1}, \code{WS}).
#'   \item Aggregating per-game rows into per-series player totals.
#'   \item Inserting new rows into \code{BattingPost}, \code{PitchingPost},
#'     and \code{SeriesPost}.
#' }
#'
#' **Attribution:** Data are from Retrosheet.  You are free to use, sell, or
#' build products from Retrosheet data provided the following notice appears
#' prominently: *"The information used here was obtained free of charge from
#' and is copyrighted by Retrosheet. Interested parties may contact Retrosheet
#' at \url{https://www.retrosheet.org}"*.
#'
#' @param con A writable \code{DBIConnection} to the baseball DuckDB database.
#'   The database must already contain \code{People} and \code{Teams} tables
#'   (loaded by \code{\link{setup_baseball_db}}).
#' @param years Integer vector of seasons to load.  Defaults to all seasons
#'   after the current maximum \code{yearID} in \code{BattingPost} (typically
#'   2022:2025 when Lahman is current through 2021).
#' @param zip_path Path to a pre-downloaded \code{basiccsvs.zip} file.  When
#'   \code{NULL} (default) the file is downloaded to \code{tempdir()} if not
#'   already cached there.
#' @param overwrite Logical.  When \code{TRUE}, delete any existing rows for
#'   the requested \code{years} in all three tables before inserting.  Default
#'   \code{FALSE} skips years already present.
#'
#' @return Invisibly returns \code{con}.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db(read_only = FALSE)
#' load_retrosheet_post(con)             # extend through latest available year
#' load_retrosheet_post(con, years = 2024)  # single season
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
load_retrosheet_post <- function(con,
                                  years     = NULL,
                                  zip_path  = NULL,
                                  overwrite = FALSE) {
  needed <- c("People", "Teams", "BattingPost", "PitchingPost", "SeriesPost")
  missing_tbls <- setdiff(needed, DBI::dbListTables(con))
  if (length(missing_tbls)) {
    stop("Required tables missing from database: ",
         paste(missing_tbls, collapse = ", "),
         "\n  Run setup_baseball_db() first.", call. = FALSE)
  }

  # Determine which years to load -----------------------------------------------
  existing_max <- DBI::dbGetQuery(
    con, "SELECT COALESCE(MAX(yearID), 2021) AS m FROM BattingPost"
  )$m
  if (is.null(years)) years <- seq.int(existing_max + 1L, 2025L)
  years <- as.integer(years)
  if (!length(years)) {
    message("  No new postseason years to load.")
    return(invisible(con))
  }

  if (!overwrite) {
    already <- DBI::dbGetQuery(
      con,
      paste0("SELECT DISTINCT yearID FROM BattingPost WHERE yearID IN (",
             paste(years, collapse = ","), ")")
    )$yearID
    years <- setdiff(years, already)
    if (!length(years)) {
      message("  BattingPost already contains all requested years.  ",
              "Use overwrite = TRUE to reload.")
      return(invisible(con))
    }
  }

  yr_min <- min(years)
  yr_max <- max(years)
  message(sprintf("Loading Retrosheet postseason data for %d-%d...", yr_min, yr_max))

  # Find / download zip ---------------------------------------------------------
  if (is.null(zip_path)) {
    zip_path <- file.path(tempdir(), "retrosheet_basiccsvs.zip")
    if (!file.exists(zip_path)) {
      message("  Downloading Retrosheet basiccsvs.zip ...")
      utils::download.file(
        "https://www.retrosheet.org/downloads/basiccsvs.zip",
        zip_path, mode = "wb", quiet = FALSE
      )
    }
  }
  if (!file.exists(zip_path))
    stop("zip_path does not exist: ", zip_path, call. = FALSE)

  # Unzip -----------------------------------------------------------------------
  extract_dir <- file.path(tempdir(), "retrosheet_csv")
  dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)
  utils::unzip(zip_path, files = c("batting.csv", "pitching.csv"),
               exdir = extract_dir, overwrite = TRUE)

  bat_csv <- file.path(extract_dir, "batting.csv")
  pit_csv <- file.path(extract_dir, "pitching.csv")

  if (!file.exists(bat_csv) || !file.exists(pit_csv))
    stop("Expected batting.csv and pitching.csv not found after unzip.", call. = FALSE)

  # Shared constants ------------------------------------------------------------
  # Retrosheet gametype values for postseason rounds.
  post_types <- "('worldseries','lcs','divisionseries','wildcard')"
  # Year filter uses integer division (//) -- DuckDB's / on BIGINT returns DOUBLE.
  yr_filter  <- paste0("date // 10000 IN (", paste(years, collapse = ","), ")")

  # Round-code SQL snippet (used in all three table queries) --------------------
  # Maps Retrosheet gametype + league -> Lahman round code.
  # Non-WS series within each (year, league, gametype) are numbered 1, 2, 3
  # by the alphabetically ordered canonical pair (LEAST(team,opp), GREATEST(...)).
  round_cte_sql <- function() {
    # Teams only covers through 2021 in the Lahman release.  Use each team's
    # most-recent available lgID as a stable proxy (teams very rarely change
    # leagues).  No year join needed.
    "
  team_lg AS (
    SELECT teamID, lgID
    FROM (
      SELECT teamID, lgID,
             ROW_NUMBER() OVER (PARTITION BY teamID ORDER BY yearID DESC) AS rn
      FROM   Teams
    )
    WHERE rn = 1
  ),
  -- Number series within (year, gametype, league) for ALDS1/ALDS2 etc.
  -- pair_low is always within one league for non-WS series.
  series_num AS (
    SELECT DISTINCT yearID, gametype, pair_low, pair_high,
      DENSE_RANK() OVER (
        PARTITION BY yearID, gametype, tl.lgID
        ORDER BY pair_low, pair_high
      ) AS sn
    FROM   src
    JOIN   team_lg tl ON tl.teamID = src.pair_low
    WHERE  gametype <> 'worldseries'
    UNION ALL
    SELECT DISTINCT yearID, gametype, pair_low, pair_high, 1 AS sn
    FROM   src
    WHERE  gametype = 'worldseries'
  ),
  -- Attach round code + lgID to every source row.
  augmented AS (
    SELECT
      src.*,
      tl.lgID,
      CASE src.gametype
        WHEN 'worldseries'    THEN 'WS'
        WHEN 'lcs'            THEN
          CASE tl.lgID WHEN 'AL' THEN 'ALCS' ELSE 'NLCS' END
        WHEN 'divisionseries' THEN
          CASE tl.lgID
            WHEN 'AL' THEN 'ALDS' || CAST(sn.sn AS VARCHAR)
            ELSE            'NLDS' || CAST(sn.sn AS VARCHAR)
          END
        WHEN 'wildcard'       THEN
          CASE tl.lgID
            WHEN 'AL' THEN 'ALWC' || CAST(sn.sn AS VARCHAR)
            ELSE            'NLWC' || CAST(sn.sn AS VARCHAR)
          END
      END AS round
    FROM   src
    JOIN   team_lg tl ON tl.teamID = src.team
    JOIN   series_num sn
           ON  sn.yearID    = src.yearID
           AND sn.gametype  = src.gametype
           AND sn.pair_low  = src.pair_low
           AND sn.pair_high = src.pair_high
  )"
  }

  # ── BattingPost ──────────────────────────────────────────────────────────────
  message("  Building BattingPost supplement...")
  bat_sql <- paste0("
  WITH
  src AS (
    SELECT gid, id, team, opp,
           (date // 10000)::INTEGER  AS yearID,
           gametype,
           LEAST(team, opp)          AS pair_low,
           GREATEST(team, opp)       AS pair_high,
           COALESCE(b_ab,  0)        AS b_ab,
           COALESCE(b_r,   0)        AS b_r,
           COALESCE(b_h,   0)        AS b_h,
           COALESCE(b_d,   0)        AS b_d,
           COALESCE(b_t,   0)        AS b_t,
           COALESCE(b_hr,  0)        AS b_hr,
           COALESCE(b_rbi, 0)        AS b_rbi,
           COALESCE(b_sh,  0)        AS b_sh,
           COALESCE(b_sf,  0)        AS b_sf,
           COALESCE(b_hbp, 0)        AS b_hbp,
           COALESCE(b_w,   0)        AS b_w,
           COALESCE(b_iw,  0)        AS b_iw,
           COALESCE(b_k,   0)        AS b_k,
           COALESCE(b_sb,  0)        AS b_sb,
           COALESCE(b_cs,  0)        AS b_cs,
           COALESCE(b_gdp, 0)        AS b_gdp
    FROM   read_csv_auto('", bat_csv, "', sample_size = -1, ignore_errors = TRUE)
    WHERE  gametype IN ", post_types, "
      AND  ", yr_filter, "
  ),",
  round_cte_sql(), "
  SELECT
    p.playerID                      AS playerID,
    a.yearID                        AS yearID,
    a.round                         AS round,
    a.team                          AS teamID,
    a.lgID                          AS lgID,
    COUNT(DISTINCT a.gid)           AS G,
    SUM(a.b_ab)                     AS AB,
    SUM(a.b_r)                      AS R,
    SUM(a.b_h)                      AS H,
    SUM(a.b_hr)                     AS HR,
    SUM(a.b_rbi)                    AS RBI,
    SUM(a.b_sb)                     AS SB,
    SUM(a.b_cs)                     AS CS,
    SUM(a.b_w)                      AS BB,
    SUM(a.b_k)                      AS SO,
    SUM(a.b_iw)                     AS IBB,
    SUM(a.b_hbp)                    AS HBP,
    SUM(a.b_sh)                     AS SH,
    SUM(a.b_sf)                     AS SF,
    SUM(a.b_gdp)                    AS GIDP,
    SUM(a.b_d)                      AS X2B,
    SUM(a.b_t)                      AS X3B
  FROM   augmented a
  JOIN   People p ON p.retroID = a.id
  GROUP BY p.playerID, a.yearID, a.round, a.team, a.lgID
  ")

  if (overwrite) {
    DBI::dbExecute(con,
      paste0("DELETE FROM BattingPost WHERE yearID IN (",
             paste(years, collapse = ","), ")"))
  }
  bat_new <- DBI::dbGetQuery(con, bat_sql)
  DBI::dbAppendTable(con, "BattingPost", bat_new)
  message(sprintf("  %-25s +%d rows", "BattingPost", nrow(bat_new)))

  # ── PitchingPost ─────────────────────────────────────────────────────────────
  message("  Building PitchingPost supplement...")
  pit_sql <- paste0("
  WITH
  src AS (
    SELECT gid, id, team, opp,
           (date // 10000)::INTEGER    AS yearID,
           gametype,
           LEAST(team, opp)            AS pair_low,
           GREATEST(team, opp)         AS pair_high,
           COALESCE(wp,      0)        AS wp,
           COALESCE(lp,      0)        AS lp,
           COALESCE(save,    0)        AS p_sv,
           COALESCE(p_gs,    0)        AS p_gs,
           COALESCE(p_gf,    0)        AS p_gf,
           COALESCE(p_cg,    0)        AS p_cg,
           COALESCE(p_ipouts,0)        AS p_ipouts,
           COALESCE(p_h,     0)        AS p_h,
           COALESCE(p_er,    0)        AS p_er,
           COALESCE(p_hr,    0)        AS p_hr,
           COALESCE(p_w,     0)        AS p_w,
           COALESCE(p_iw,    0)        AS p_iw,
           COALESCE(p_k,     0)        AS p_k,
           COALESCE(p_hbp,   0)        AS p_hbp,
           COALESCE(p_wp,    0)        AS p_wp,
           COALESCE(p_bk,    0)        AS p_bk,
           COALESCE(p_bfp,   0)        AS p_bfp,
           COALESCE(p_r,     0)        AS p_r,
           COALESCE(p_sh,    0)        AS p_sh,
           COALESCE(p_sf,    0)        AS p_sf
    FROM   read_csv_auto('", pit_csv, "', sample_size = -1, ignore_errors = TRUE)
    WHERE  gametype IN ", post_types, "
      AND  ", yr_filter, "
  ),",
  round_cte_sql(), "
  SELECT
    pp.playerID                           AS playerID,
    a.yearID                              AS yearID,
    a.round                               AS round,
    a.team                                AS teamID,
    a.lgID                                AS lgID,
    SUM(a.wp)                             AS W,
    SUM(a.lp)                             AS L,
    COUNT(DISTINCT a.gid)                 AS G,
    SUM(a.p_gs)                           AS GS,
    SUM(a.p_cg)                           AS CG,
    0::BIGINT                             AS SHO,
    SUM(a.p_sv)                           AS SV,
    SUM(a.p_ipouts)                       AS IPouts,
    SUM(a.p_h)                            AS H,
    SUM(a.p_er)                           AS ER,
    SUM(a.p_hr)                           AS HR,
    SUM(a.p_w)                            AS BB,
    SUM(a.p_k)                            AS SO,
    -- BAOpp: H / (BFP - BB - IBB - HBP - SH - SF)
    SUM(a.p_h)::DOUBLE /
      NULLIF(SUM(a.p_bfp) - SUM(a.p_w) - SUM(a.p_iw)
             - SUM(a.p_hbp) - SUM(a.p_sh) - SUM(a.p_sf),
             0)                           AS BAOpp,
    -- ERA: ER * 27 / IPouts
    SUM(a.p_er)::DOUBLE * 27.0 /
      NULLIF(SUM(a.p_ipouts), 0)          AS ERA,
    SUM(a.p_iw)                           AS IBB,
    SUM(a.p_wp)                           AS WP,
    SUM(a.p_hbp)                          AS HBP,
    SUM(a.p_bk)                           AS BK,
    SUM(a.p_bfp)                          AS BFP,
    SUM(a.p_gf)                           AS GF,
    SUM(a.p_r)                            AS R,
    SUM(a.p_sh)                           AS SH,
    SUM(a.p_sf)                           AS SF,
    0::BIGINT                             AS GIDP
  FROM   augmented a
  JOIN   People pp ON pp.retroID = a.id
  GROUP BY pp.playerID, a.yearID, a.round, a.team, a.lgID
  ")

  if (overwrite) {
    DBI::dbExecute(con,
      paste0("DELETE FROM PitchingPost WHERE yearID IN (",
             paste(years, collapse = ","), ")"))
  }
  pit_new <- DBI::dbGetQuery(con, pit_sql)
  DBI::dbAppendTable(con, "PitchingPost", pit_new)
  message(sprintf("  %-25s +%d rows", "PitchingPost", nrow(pit_new)))

  # ── SeriesPost ───────────────────────────────────────────────────────────────
  # Derive series outcomes from game-level data.  Home-team rows (vishome='h')
  # give exactly one win/loss record per game without double-counting.
  message("  Building SeriesPost supplement...")
  ser_sql <- paste0("
  WITH
  src AS (
    SELECT DISTINCT gid, team, opp, vishome, win,
           (date // 10000)::INTEGER  AS yearID,
           gametype,
           LEAST(team, opp)          AS pair_low,
           GREATEST(team, opp)       AS pair_high
    FROM   read_csv_auto('", bat_csv, "', sample_size = -1, ignore_errors = TRUE)
    WHERE  gametype IN ", post_types, "
      AND  ", yr_filter, "
      AND  vishome = 'h'
  ),",
  round_cte_sql(), ",
  -- Count wins per team per series using home-team perspective.
  -- Home team's win=1 means visitor lost; invert for visitor.
  home_side AS (
    SELECT yearID, round, team, opp,
           SUM(win)       AS wins,
           SUM(1 - win)   AS losses
    FROM   augmented
    GROUP BY yearID, round, team, opp
  ),
  vis_side AS (
    SELECT yearID, round, opp AS team, team AS opp,
           SUM(1 - win)   AS wins,
           SUM(win)        AS losses
    FROM   augmented
    GROUP BY yearID, round, opp, team
  ),
  totals AS (
    SELECT yearID, round, team,
           SUM(wins)   AS wins,
           SUM(losses) AS losses
    FROM   (SELECT yearID, round, team, wins, losses FROM home_side
            UNION ALL
            SELECT yearID, round, team, wins, losses FROM vis_side)
    GROUP BY yearID, round, team
  ),
  -- Pair teams; winner = higher wins (ties go to the larger-code team as
  -- a tie-break but real ties are very rare in postseason play).
  paired AS (
    SELECT
      t1.yearID, t1.round,
      CASE WHEN t1.wins >= t2.wins THEN t1.team ELSE t2.team END AS teamIDwinner,
      CASE WHEN t1.wins >= t2.wins THEN t2.team ELSE t1.team END AS teamIDloser,
      GREATEST(t1.wins, t2.wins)::BIGINT                         AS wins,
      LEAST(t1.wins,    t2.wins)::BIGINT                         AS losses,
      0::BIGINT                                                   AS ties
    FROM   totals t1
    JOIN   totals t2
           ON  t1.yearID = t2.yearID AND t1.round = t2.round
           AND t1.team   < t2.team
  ),
  lg AS (
    SELECT teamID, lgID
    FROM (
      SELECT teamID, lgID,
             ROW_NUMBER() OVER (PARTITION BY teamID ORDER BY yearID DESC) AS rn
      FROM   Teams
    )
    WHERE rn = 1
  )
  SELECT
    p.yearID,
    p.round,
    p.teamIDwinner,
    w.lgID            AS lgIDwinner,
    p.teamIDloser,
    l.lgID            AS lgIDloser,
    p.wins,
    p.losses,
    p.ties
  FROM   paired p
  JOIN   lg w ON w.teamID = p.teamIDwinner
  JOIN   lg l ON l.teamID = p.teamIDloser
  ")

  if (overwrite) {
    DBI::dbExecute(con,
      paste0("DELETE FROM SeriesPost WHERE yearID IN (",
             paste(years, collapse = ","), ")"))
  }
  ser_new <- DBI::dbGetQuery(con, ser_sql)
  DBI::dbAppendTable(con, "SeriesPost", ser_new)
  message(sprintf("  %-25s +%d rows", "SeriesPost", nrow(ser_new)))

  # Cleanup extracted CSVs (zip kept for reuse) ---------------------------------
  unlink(c(bat_csv, pit_csv))

  message(sprintf("\nRetrosheet postseason data loaded for years: %s",
                  paste(sort(years), collapse = ", ")))
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
