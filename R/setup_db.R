#' Build the baseball DuckDB database
#'
#' Writes all Lahman package tables plus scraped USA Today salary data into a
#' persistent DuckDB file, then creates a `SalariesAll` view that unions both
#' salary sources on a common schema.
#'
#' @param dbdir Path for the output `baseball.duckdb` file. Defaults to the
#'   value of the `LAHMANS_DBDIR` environment variable if set, otherwise
#'   `~/Documents/Data/baseball/baseball.duckdb`.
#' @param sal_file Path to the combined USA Today salary CSV produced by
#'   [scrape_salaries()]. When `NULL` (default), looks for
#'   `salaries_*_with_playerID.csv` in the same directory as `dbdir`. USA
#'   Today data is not bundled with the package — users must run
#'   [scrape_salaries()] to obtain it.
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
setup_baseball_db <- function(dbdir    = NULL,
                               sal_file = NULL,
                               overwrite = FALSE) {
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

  # -- USA Today scraped salaries -----------------------------------------------
  if (is.null(sal_file)) {
    data_dir <- dirname(dbdir)
    candidates <- list.files(data_dir, pattern = "salaries.*with_playerID\\.csv$",
                             full.names = TRUE)
    sal_file <- if (length(candidates)) candidates[[1L]] else ""
  }
  if (!nzchar(sal_file) || !file.exists(sal_file)) {
    warning("Scraped salary file not found -- SalariesUSAToday will not be loaded.\n",
            "Run scrape_salaries() then rerun setup_baseball_db(sal_file = <path>).")
  } else {
    sal <- data.table::fread(sal_file)
    sal[, average_annual := as.numeric(gsub("[$,]", "", average_annual))]
    DBI::dbWriteTable(con, "SalariesUSAToday", sal, overwrite = TRUE)
    message(sprintf("  %-25s %d rows", "SalariesUSAToday", nrow(sal)))

    # -- SalariesAll view: Lahman (<=2016) + USA Today expanded with imputed years --
    DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW SalariesAll AS
      WITH

      -- Parse each USA Today row: clean AAV and extract contract start/end year.
      -- Handles patterns: 'N (YYYY-YY)', 'NN (YYYY-YY)', 'N(YYYY-YY)'.
      -- Rows with NULL years (1-year deals) get NULL c_start/c_end and pass
      -- through as actual records via the FULL JOIN below.
      usa_parsed AS (
        SELECT
          playerID,
          team                                                              AS teamID,
          salary::DOUBLE                                                    AS salary,
          yearID,
          average_annual::DOUBLE                                            AS aav,
          TRY_CAST(regexp_extract(years, '\\((\\d{4})-', 1) AS INTEGER)    AS c_start,
          CASE
            WHEN regexp_extract(years, '-(\\d{4})\\)', 1) <> ''
              THEN TRY_CAST(regexp_extract(years, '-(\\d{4})\\)', 1) AS INTEGER)
            WHEN regexp_extract(years, '-(\\d{2})\\)', 1) <> ''
              THEN TRY_CAST(
                     left(regexp_extract(years, '\\((\\d{4})-', 1), 2) ||
                     regexp_extract(years, '-(\\d{2})\\)', 1)
                   AS INTEGER)
          END                                                               AS c_end
        FROM SalariesUSAToday
        WHERE playerID IS NOT NULL
      ),

      -- One actual record per player-season (handles any duplicate scrapes).
      actual AS (
        SELECT DISTINCT ON (playerID, yearID)
          playerID, teamID, salary, yearID
        FROM usa_parsed
        ORDER BY playerID, yearID
      ),

      -- One contract per player + contract span; most-recent scrape's AAV wins
      -- when the same contract appears in multiple scraped years.
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

      -- Expand each contract to one row per year in the contract range.
      contract_years AS (
        SELECT c.playerID, c.teamID, c.aav, gs::INTEGER AS yearID
        FROM contracts c,
          LATERAL (SELECT unnest(generate_series(c.c_start, c.c_end)) AS gs) t
      ),

      -- Merge contract-year grid with actual records.
      --   * Matching rows  -> use actual salary (is_actual = TRUE)
      --   * Contract only  -> impute with AAV    (is_actual = FALSE)
      --   * Actual only    -> 1-year deals / NULL years pass through as-is
      -- DISTINCT ON deduplicates the rare case where overlapping contracts
      -- produce two rows for the same player-season; actual wins.
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
      )

      -- Lahman (authoritative through 2016)
      SELECT playerID, yearID, teamID, lgID,
             salary::DOUBLE AS salary,
             'lahman'       AS source,
             TRUE           AS is_actual
      FROM   Salaries

      UNION ALL

      -- USA Today: actual salaries (2022-2025) + AAV-imputed contract years
      SELECT playerID, yearID, teamID, NULL AS lgID,
             salary,
             'usatoday'     AS source,
             is_actual
      FROM   usa_expanded
    ")
    message(sprintf("  %-25s (view)", "SalariesAll"))
  }

  # ── Stats views ──────────────────────────────────────────────────────────────
  create_stats_views(con)

  n <- length(DBI::dbListTables(con))
  message(sprintf("\nDone. %d tables/views written to %s", n, dbdir))
  invisible(dbdir)
}
