#' Build the baseball DuckDB database
#'
#' Downloads all 27 Lahman baseball tables directly from the
#' [Chadwick Bureau baseballdatabank](https://github.com/cbwinslow/baseballdatabank)
#' via DuckDB's httpfs extension and writes them into a persistent DuckDB file,
#' then creates a `SalariesAll` view that unions all salary sources on a common
#' schema:
#' - **Chadwick Bureau** (`Salaries`): authoritative 1985--2016
#' - **Spotrac** (`SalariesSpotrac`): player-level actuals 2017--2021
#' - **USA Today** (`SalariesUSAToday`): player-level actuals 2022--2025
#'
#' Requires an internet connection to download the base tables. Optional tables
#' that fail to download are skipped with a warning; required tables (People,
#' Batting, Pitching, Fielding, Teams, Salaries) raise an error.
#'
#' Optionally fetches supplemental data via \pkg{baseballr}:
#' - `load_chadwick = TRUE` downloads the Chadwick Bureau player ID crosswalk
#'   and creates the `PlayerIDs` view (ODC-BY 1.0 licensed; safe to use locally).
#' - `load_war = TRUE` additionally fetches FanGraphs WAR leaderboards and
#'   creates the `PlayerWAR` and `SalaryPerWAR` views.  Implies
#'   `load_chadwick = TRUE`.  Both batting and pitching WAR are available
#'   from FanGraphs for the full salary era (1985+).
#'
#' @param dbdir Path for the output `baseball.duckdb` file. Defaults to the
#'   value of the `LAHMANS_DBDIR` environment variable if set, otherwise
#'   `~/Documents/Data/baseball/baseball.duckdb`.
#' @param sal_file Path to the combined USA Today salary CSV produced by
#'   [scrape_salaries()]. When `NULL` (default), looks for
#'   `salaries_*_with_playerID.csv` (non-Spotrac) in the same directory as
#'   `dbdir`. USA Today data is not bundled -- users must run
#'   [scrape_salaries()] to obtain it.
#' @param spotrac_file Path to the combined Spotrac salary CSV produced by
#'   `data-raw/salaries.R`. When `NULL` (default), looks for
#'   `salaries_spotrac_*_with_playerID.csv` in the same directory as `dbdir`.
#'   Spotrac data is not bundled -- users must run `data-raw/salaries.R` to
#'   obtain it.
#' @param overwrite If `TRUE`, drop and recreate existing tables. Default
#'   `FALSE` aborts if the file already exists.
#' @param load_chadwick If `TRUE`, download the Chadwick Bureau player ID
#'   crosswalk via \pkg{baseballr} and create the `PlayerIDs` view.
#'   Requires an internet connection and \pkg{baseballr}.  Default `FALSE`.
#' @param load_war If `TRUE`, fetch FanGraphs WAR leaderboards and create
#'   `PlayerWAR` and `SalaryPerWAR` views.  Implies `load_chadwick = TRUE`.
#'   Requires an internet connection and \pkg{baseballr}.  Default `FALSE`.
#' @param war_years Integer vector of seasons to fetch for WAR data.
#'   Defaults to `1985:2025` (full salary era).
#' @param load_retrosheet If `TRUE`, download Retrosheet postseason CSVs and
#'   extend `BattingPost`, `PitchingPost`, and `SeriesPost` through the latest
#'   available year (currently 2025).  The Lahman tables stop at 2021; this
#'   fills the gap.  Requires an internet connection.  Defaults to `TRUE` when
#'   `load_war = TRUE`, `FALSE` otherwise.
#' @param retrosheet_zip Optional path to a pre-downloaded Retrosheet
#'   `basiccsvs.zip`.  When `NULL` (default), the file is downloaded from
#'   \url{https://www.retrosheet.org/downloads/basiccsvs.zip}.
#'
#' @return Invisibly returns `dbdir`.
#' @export
#'
#' @examples
#' \dontrun{
#' # Download all tables from Chadwick Bureau and build database
#' setup_baseball_db()
#'
#' # With full WAR and postseason coverage through 2025
#' setup_baseball_db(load_war = TRUE, overwrite = TRUE)
#' # load_war = TRUE implies load_retrosheet = TRUE automatically
#' }
setup_baseball_db <- function(dbdir         = NULL,
                               sal_file      = NULL,
                               spotrac_file  = NULL,
                               overwrite     = FALSE,
                               load_chadwick = FALSE,
                               load_war      = FALSE,
                               war_years     = 1985:2025,
                               load_retrosheet = load_war,
                               retrosheet_zip  = NULL) {
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

  # -- Install httpfs and load baseballdatabank CSVs ---------------------------
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs")

  base_url <- "https://raw.githubusercontent.com/cbwinslow/baseballdatabank/master"

  # Table catalog: name -> list(subdir, required)
  tbl_catalog <- list(
    # Core tables (19)
    AllstarFull        = list(subdir = "core",    required = FALSE),
    Appearances        = list(subdir = "core",    required = FALSE),
    Batting            = list(subdir = "core",    required = TRUE),
    BattingPost        = list(subdir = "core",    required = FALSE),
    Fielding           = list(subdir = "core",    required = TRUE),
    FieldingOF         = list(subdir = "core",    required = FALSE),
    FieldingOFsplit    = list(subdir = "core",    required = FALSE),
    FieldingPost       = list(subdir = "core",    required = FALSE),
    HomeGames          = list(subdir = "core",    required = FALSE),
    Managers           = list(subdir = "core",    required = FALSE),
    ManagersHalf       = list(subdir = "core",    required = FALSE),
    Parks              = list(subdir = "core",    required = FALSE),
    People             = list(subdir = "core",    required = TRUE),
    Pitching           = list(subdir = "core",    required = TRUE),
    PitchingPost       = list(subdir = "core",    required = FALSE),
    SeriesPost         = list(subdir = "core",    required = FALSE),
    Teams              = list(subdir = "core",    required = TRUE),
    TeamsFranchises    = list(subdir = "core",    required = FALSE),
    TeamsHalf          = list(subdir = "core",    required = FALSE),
    # Contrib tables (8)
    AwardsManagers     = list(subdir = "contrib", required = FALSE),
    AwardsPlayers      = list(subdir = "contrib", required = FALSE),
    AwardsShareManagers = list(subdir = "contrib", required = FALSE),
    AwardsSharePlayers = list(subdir = "contrib", required = FALSE),
    CollegePlaying     = list(subdir = "contrib", required = FALSE),
    HallOfFame         = list(subdir = "contrib", required = FALSE),
    Salaries           = list(subdir = "contrib", required = TRUE),
    Schools            = list(subdir = "contrib", required = FALSE)
  )

  # Tables that need 2B/3B column renames
  rename_2b3b <- c("Batting", "BattingPost")

  invisible(lapply(names(tbl_catalog), function(nm) {
    info <- tbl_catalog[[nm]]
    url  <- sprintf("%s/%s/%s.csv", base_url, info$subdir, nm)

    select_expr <- if (nm %in% rename_2b3b) {
      sprintf('SELECT * EXCLUDE ("2B", "3B"), "2B" AS X2B, "3B" AS X3B FROM read_csv_auto(\'%s\', sample_size=-1)', url)
    } else {
      sprintf("SELECT * FROM read_csv_auto('%s', sample_size=-1)", url)
    }

    tryCatch({
      DBI::dbExecute(con, sprintf('CREATE OR REPLACE TABLE "%s" AS %s', nm, select_expr))
      n <- DBI::dbGetQuery(con, sprintf('SELECT COUNT(*) AS n FROM "%s"', nm))$n
      message(sprintf("  %-25s %d rows", nm, n))
    }, error = function(e) {
      if (info$required) {
        stop(sprintf("Required table '%s' failed to load from %s.\n  Check internet connection.\n  Error: %s",
                     nm, url, conditionMessage(e)), call. = FALSE)
      } else {
        warning(sprintf("Optional table '%s' failed to load from %s -- skipping.\n  Error: %s",
                        nm, url, conditionMessage(e)), call. = FALSE)
      }
    })
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
          -- Extract year parts once so the CASE below can reference them cleanly
          TRY_CAST(regexp_extract(years, '\\((\\d{4})-', 1) AS INTEGER)    AS y_start,
          regexp_extract(years, '-(\\d{4})\\)', 1)                         AS y_end_4,
          TRY_CAST(regexp_extract(years, '-(\\d{2})\\)', 1) AS INTEGER)    AS y_end_2
        FROM SalariesUSAToday
        WHERE playerID IS NOT NULL
      ),
      usa_parsed2 AS (
        SELECT
          playerID, teamID, salary, yearID, aav,
          y_start                                                           AS c_start,
          CASE
            WHEN y_end_4 <> ''
              THEN TRY_CAST(y_end_4 AS INTEGER)
            WHEN y_end_2 IS NOT NULL
              -- Century-safe: base century from c_start + 100 if 2-digit end wraps
              THEN (
                     (y_start / 100) * 100
                     + y_end_2
                     + CASE WHEN y_end_2 < y_start % 100 THEN 100 ELSE 0 END
                   )::INTEGER
          END                                                               AS c_end
        FROM usa_parsed
      ),
      actual AS (
        SELECT DISTINCT ON (playerID, yearID)
          playerID, teamID, salary, yearID
        FROM usa_parsed2
        ORDER BY playerID, yearID
      ),
      contracts AS (
        SELECT DISTINCT ON (playerID, c_start, c_end)
          playerID, teamID, aav, c_start, c_end
        FROM usa_parsed2
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
  } else {
    # Lahman Salaries (1985-2016) only -- fallback when no supplemental files loaded
    DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW SalariesAll AS
      SELECT playerID, yearID, teamID, lgID,
             salary::DOUBLE AS salary,
             'lahman'       AS source,
             TRUE           AS is_actual
      FROM   Salaries
    ")
    message(sprintf("  %-25s (view, Lahman only -- no supplemental salary files)", "SalariesAll"))
  }

  # ── Stats views ──────────────────────────────────────────────────────────────
  create_stats_views(con)

  # ── Optional supplemental loaders ────────────────────────────────────────────
  # load_war implies load_chadwick (WAR join requires the Chadwick crosswalk)
  if (load_war && !load_chadwick) load_chadwick <- TRUE
  if (load_chadwick) load_chadwick_ids(con, overwrite = overwrite)
  if (load_war)      load_fangraphs_war(con, years = war_years, overwrite = overwrite)
  if (load_retrosheet) load_retrosheet_post(con, zip_path = retrosheet_zip,
                                            overwrite = overwrite)

  n <- length(DBI::dbListTables(con))
  message(sprintf("\nDone. %d tables/views written to %s", n, dbdir))
  invisible(dbdir)
}
