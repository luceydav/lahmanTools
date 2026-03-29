#' Build the baseball DuckDB database
#'
#' Writes all Lahman package tables plus scraped salary data into a persistent
#' DuckDB file, then creates a `SalariesAll` view that unions all salary
#' sources on a common schema:
#' - **Lahman** (`Salaries`): authoritative 1985–2016
#' - **Spotrac** (`SalariesSpotrac`): player-level actuals 2017–2021
#' - **USA Today** (`SalariesUSAToday`): player-level actuals 2022–2025
#'
#' @param dbdir Path for the output `baseball.duckdb` file. Defaults to the
#'   value of the `LAHMANS_DBDIR` environment variable if set, otherwise
#'   `~/Documents/Data/baseball/baseball.duckdb`.
#' @param sal_file Path to the combined USA Today salary CSV produced by
#'   [scrape_salaries()]. When `NULL` (default), looks for
#'   `salaries_*_with_playerID.csv` (non-Spotrac) in the same directory as
#'   `dbdir`. USA Today data is not bundled — users must run
#'   [scrape_salaries()] to obtain it.
#' @param spotrac_file Path to the combined Spotrac salary CSV produced by
#'   `data-raw/salaries.R`. When `NULL` (default), looks for
#'   `salaries_spotrac_*_with_playerID.csv` in the same directory as `dbdir`.
#'   Spotrac data is not bundled — users must run `data-raw/salaries.R` to
#'   obtain it.
#' @param overwrite If `TRUE`, drop and recreate existing tables. Default
#'   `FALSE` aborts if the file already exists.
#'
#' @return Invisibly returns `dbdir`.
#' @export
#'
#' @examples
#' \dontrun{
#' setup_baseball_db()
#' }
setup_baseball_db <- function(dbdir         = NULL,
                               sal_file      = NULL,
                               spotrac_file  = NULL,
                               overwrite     = FALSE) {
  if (is.null(dbdir)) {
    dbdir <- Sys.getenv(
      "LAHMANS_DBDIR",
      unset = path.expand("~/Documents/Data/baseball/baseball.duckdb")
    )
  }
  if (file.exists(dbdir) && !overwrite) {
    stop(dbdir, " already exists. Use overwrite = TRUE to rebuild.")
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # -- Lahman tables -----------------------------------------------------------
  skip    <- c("LahmanData", "battingLabels", "fieldingLabels", "pitchingLabels")
  tbl_names <- setdiff(data(package = "Lahman")$results[, "Item"], skip)

  invisible(lapply(tbl_names, function(nm) {
    e <- new.env(parent = emptyenv())
    utils::data(list = nm, package = "Lahman", envir = e)
    dt <- data.table::as.data.table(e[[nm]])
    dt_factors_to_char(dt)
    DBI::dbWriteTable(con, nm, dt, overwrite = TRUE)
    message(sprintf("  %-25s %d rows", nm, nrow(dt)))
  }))

  # -- Spotrac scraped salaries (2017-2021) -------------------------------------
  if (is.null(spotrac_file)) {
    data_dir <- dirname(dbdir)
    candidates <- list.files(data_dir,
                             pattern = "salaries_spotrac_.*with_playerID\\.csv$",
                             full.names = TRUE)
    spotrac_file <- if (length(candidates)) candidates[[1L]] else ""
  }
  has_spotrac <- nzchar(spotrac_file) && file.exists(spotrac_file)
  if (!has_spotrac) {
    message("  Spotrac salary file not found -- SalariesSpotrac will not be loaded.\n",
            "  Run data-raw/salaries.R then rerun setup_baseball_db(spotrac_file = <path>).")
  } else {
    sp <- data.table::fread(spotrac_file)
    DBI::dbWriteTable(con, "SalariesSpotrac", sp, overwrite = TRUE)
    message(sprintf("  %-25s %d rows", "SalariesSpotrac", nrow(sp)))
  }

  # -- USA Today scraped salaries (2022+) ---------------------------------------
  if (is.null(sal_file)) {
    data_dir <- dirname(dbdir)
    candidates <- list.files(data_dir, pattern = "salaries.*with_playerID\\.csv$",
                             full.names = TRUE)
    candidates <- candidates[!grepl("spotrac", candidates, fixed = TRUE)]
    sal_file <- if (length(candidates)) candidates[[1L]] else ""
  }
  has_usatoday <- nzchar(sal_file) && file.exists(sal_file)
  if (!has_usatoday) {
    warning("USA Today salary file not found -- SalariesUSAToday will not be loaded.\n",
            "Run scrape_salaries() then rerun setup_baseball_db(sal_file = <path>).")
  } else {
    sal <- data.table::fread(sal_file)
    sal[, average_annual := as.numeric(gsub("[$,]", "", average_annual))]
    DBI::dbWriteTable(con, "SalariesUSAToday", sal, overwrite = TRUE)
    message(sprintf("  %-25s %d rows", "SalariesUSAToday", nrow(sal)))
  }

  # -- SalariesAll view: union all three salary sources -------------------------
  # Only build the view if at least one supplemental source loaded
  if (has_spotrac || has_usatoday) {
    # Normalise USA Today full-name team strings to Lahman 3-letter teamIDs.
    usa_team_case <- "
          CASE team
            WHEN 'Angels'            THEN 'LAA'  WHEN 'L.A. Angels'       THEN 'LAA'
            WHEN 'Arizona'           THEN 'ARI'  WHEN 'Diamondbacks'      THEN 'ARI'
            WHEN 'Astros'            THEN 'HOU'  WHEN 'Houston'           THEN 'HOU'
            WHEN 'Athletics'         THEN 'OAK'  WHEN 'Oakland'           THEN 'OAK'
            WHEN 'Atlanta'           THEN 'ATL'  WHEN 'Braves'            THEN 'ATL'
            WHEN 'Baltimore'         THEN 'BAL'  WHEN 'Orioles'           THEN 'BAL'
            WHEN 'Blue Jays'         THEN 'TOR'  WHEN 'Toronto'           THEN 'TOR'
            WHEN 'Boston'            THEN 'BOS'  WHEN 'Red Sox'           THEN 'BOS'
            WHEN 'Brewers'           THEN 'MIL'  WHEN 'Milwaukee'         THEN 'MIL'
            WHEN 'Cardinals'         THEN 'SLN'  WHEN 'St. Louis'         THEN 'SLN'
            WHEN 'Chi. Cubs'         THEN 'CHN'  WHEN 'Chicago Cubs'      THEN 'CHN'
            WHEN 'Cubs'              THEN 'CHN'
            WHEN 'Chic. White Sox'   THEN 'CHA'  WHEN 'Chicago White Sox' THEN 'CHA'
            WHEN 'White Sox'         THEN 'CHA'
            WHEN 'Cincinnati'        THEN 'CIN'  WHEN 'Reds'              THEN 'CIN'
            WHEN 'Cleveland'         THEN 'CLE'  WHEN 'Guardians'         THEN 'CLE'
            WHEN 'Colorado'          THEN 'COL'  WHEN 'Rockies'           THEN 'COL'
            WHEN 'Detroit'           THEN 'DET'  WHEN 'Tigers'            THEN 'DET'
            WHEN 'Dodgers'           THEN 'LAN'  WHEN 'L.A. Dodgers'      THEN 'LAN'
            WHEN 'Giants'            THEN 'SFN'  WHEN 'San Francisco'     THEN 'SFN'
            WHEN 'Kansas City'       THEN 'KCA'  WHEN 'Royals'            THEN 'KCA'
            WHEN 'Mariners'          THEN 'SEA'  WHEN 'Seattle'           THEN 'SEA'
            WHEN 'Marlins'           THEN 'MIA'  WHEN 'Miami'             THEN 'MIA'
            WHEN 'Mets'              THEN 'NYN'  WHEN 'N.Y. Mets'         THEN 'NYN'
            WHEN 'Minnesota'         THEN 'MIN'  WHEN 'Twins'             THEN 'MIN'
            WHEN 'Nationals'         THEN 'WAS'  WHEN 'Washington'        THEN 'WAS'
            WHEN 'N.Y. Yankees'      THEN 'NYA'  WHEN 'Yankees'           THEN 'NYA'
            WHEN 'Philadelphia'      THEN 'PHI'  WHEN 'Phillies'          THEN 'PHI'
            WHEN 'Pittsburgh'        THEN 'PIT'  WHEN 'Pirates'           THEN 'PIT'
            WHEN 'Rangers'           THEN 'TEX'  WHEN 'Texas'             THEN 'TEX'
            WHEN 'Rays'              THEN 'TBA'  WHEN 'Tampa Bay'         THEN 'TBA'
            WHEN 'San Diego'         THEN 'SDN'  WHEN 'Padres'            THEN 'SDN'
            ELSE team
          END"

    usatoday_cte <- if (has_usatoday) paste0("
      -- Parse each USA Today row: clean AAV and extract contract start/end year.
      -- Handles patterns: 'N (YYYY-YY)', 'NN (YYYY-YY)', 'N(YYYY-YY)'.
      -- Rows with NULL years (1-year deals) get NULL c_start/c_end and pass
      -- through as actual records via the FULL JOIN below.
      usa_parsed AS (
        SELECT
          playerID,", usa_team_case, "                                        AS teamID,
          salary::DOUBLE                                                    AS salary,
          yearID,
          average_annual::DOUBLE                                            AS aav,
          TRY_CAST(regexp_extract(years, '\\((\\d{4})-', 1) AS INTEGER)    AS c_start,
          CASE
            WHEN regexp_extract(years, '-(\\d{4})\\)', 1) <> ''
              THEN TRY_CAST(regexp_extract(years, '-(\\d{4})\\)', 1) AS INTEGER)
            WHEN regexp_extract(years, '-(\\d{2})\\)', 1) <> ''
              -- Century-safe: base century from c_start + 100 if 2-digit end wraps
              THEN (
                     (TRY_CAST(regexp_extract(years, '\\((\\d{4})-', 1) AS INTEGER) / 100) * 100
                     + TRY_CAST(regexp_extract(years, '-(\\d{2})\\)', 1) AS INTEGER)
                     + CASE
                         WHEN TRY_CAST(regexp_extract(years, '-(\\d{2})\\)', 1) AS INTEGER)
                                < TRY_CAST(regexp_extract(years, '\\((\\d{4})-', 1) AS INTEGER) % 100
                         THEN 100 ELSE 0
                       END
                   )::INTEGER
          END                                                               AS c_end
        FROM SalariesUSAToday
        WHERE playerID IS NOT NULL
      ),
      actual AS (
        SELECT DISTINCT ON (playerID, yearID)
          playerID, teamID, salary, yearID
        FROM usa_parsed
        ORDER BY playerID, yearID
      ),
      contracts AS (
        SELECT DISTINCT ON (playerID, c_start, c_end)
          playerID, teamID, aav, c_start, c_end
        FROM usa_parsed
        WHERE c_start IS NOT NULL
          AND c_end   IS NOT NULL
          AND c_start <= c_end
          AND aav     IS NOT NULL
        ORDER BY playerID, c_start, c_end, yearID DESC
      ),
      contract_years AS (
        SELECT c.playerID, c.teamID, c.aav, gs::INTEGER AS yearID
        FROM contracts c,
          LATERAL (SELECT unnest(generate_series(c.c_start, c.c_end)) AS gs) t
      ),
      usa_expanded AS (
        SELECT DISTINCT ON (playerID, yearID)
          COALESCE(a.playerID, cy.playerID)::VARCHAR AS playerID,
          COALESCE(a.yearID,   cy.yearID)::INTEGER   AS yearID,
          COALESCE(a.teamID,   cy.teamID)::VARCHAR   AS teamID,
          COALESCE(a.salary,   cy.aav)::DOUBLE       AS salary,
          (a.playerID IS NOT NULL)::BOOLEAN          AS is_actual
        FROM contract_years cy
        FULL JOIN actual a
          ON cy.playerID = a.playerID AND cy.yearID = a.yearID
        ORDER BY playerID, yearID, is_actual DESC
      )") else ""

    usatoday_union <- if (has_usatoday) "
      UNION ALL
      -- USA Today: actual salaries (2022+) + AAV-imputed contract years
      SELECT playerID, yearID, teamID, NULL AS lgID,
             salary,
             'usatoday'     AS source,
             is_actual
      FROM   usa_expanded" else ""

    # Normalise Spotrac abbreviations that differ from Lahman teamIDs.
    spotrac_team_case <- "
          CASE team
            WHEN 'CHC' THEN 'CHN'  WHEN 'CHW' THEN 'CHA'
            WHEN 'KC'  THEN 'KCA'  WHEN 'LAD' THEN 'LAN'
            WHEN 'NYM' THEN 'NYN'  WHEN 'NYY' THEN 'NYA'
            WHEN 'SD'  THEN 'SDN'  WHEN 'SF'  THEN 'SFN'
            WHEN 'STL' THEN 'SLN'  WHEN 'TB'  THEN 'TBA'
            WHEN 'WSH' THEN 'WAS'
            ELSE team
          END"

    spotrac_union <- if (has_spotrac) paste0("
      UNION ALL
      -- Spotrac: actual player salaries 2017-2021, filtered to MLB-contract players
      -- (salary >= MLB minimum for that year) for consistency with Lahman.
      -- Minor leaguers on 40-man rosters are excluded.
      -- MLB minimums: 2017=$535K, 2018=$545K, 2019=$555K, 2020=$208K(prorated 60g), 2021=$570.5K
      SELECT playerID::VARCHAR,
             yearID::INTEGER,", spotrac_team_case, "::VARCHAR  AS teamID,
             NULL::VARCHAR    AS lgID,
             salary::DOUBLE,
             'spotrac'        AS source,
             TRUE             AS is_actual
      FROM   SalariesSpotrac
      WHERE  playerID IS NOT NULL
        AND  salary >= CASE yearID
               WHEN 2017 THEN 535000
               WHEN 2018 THEN 545000
               WHEN 2019 THEN 555000
               WHEN 2020 THEN 208000
               WHEN 2021 THEN 570500
               ELSE 500000
             END") else ""

    # Comma after last CTE only if usa CTEs are present
    cte_comma <- if (has_usatoday) "," else ""

    DBI::dbExecute(con, paste0("
      CREATE OR REPLACE VIEW SalariesAll AS
      WITH
      -- placeholder CTE so the WITH clause is always valid even with no usa CTEs
      _dummy AS (SELECT 1 AS x)", cte_comma,
      usatoday_cte, "

      -- Lahman (authoritative through 2016)
      SELECT playerID, yearID, teamID, lgID,
             salary::DOUBLE AS salary,
             'lahman'       AS source,
             TRUE           AS is_actual
      FROM   Salaries",
      spotrac_union,
      usatoday_union
    ))
    message(sprintf("  %-25s (view)", "SalariesAll"))
  }

  # ── Stats views ──────────────────────────────────────────────────────────────
  create_stats_views(con)

  n <- length(DBI::dbListTables(con))
  message(sprintf("\nDone. %d tables/views written to %s", n, dbdir))
  message("Tip: run write_mcp_config() to configure AI tools (Copilot CLI, Claude Code) ",
          "to query this database.")
  invisible(dbdir)
}
