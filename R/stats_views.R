#' Create per-season stats views in the baseball DuckDB database
#'
#' Adds views and a scalar SQL macro that extend the raw Lahman tables with
#' derived statistics. The raw tables are never modified.
#'
#' **Per-player views** (one row per player-year-stint-team):
#'
#' | View | Base table | Key metrics added |
#' |------|------------|-------------------|
#' | `BattingStats`  | `Batting`  | PA, AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K% |
#' | `PitchingStats` | `Pitching` | IP, WHIP, K/9, BB/9, HR/9, H/9, K/BB, FIP, FIP_constant, Win% |
#' | `FieldingStats` | `Fielding` | FPCT, RF/9, RF/G |
#'
#' **Analytical views** (pre-built patterns used across analysis queries):
#'
#' | View | Base tables | Description |
#' |------|-------------|-------------|
#' | `PlayerAcquisitionType` | `Batting`, `Pitching`, `People` | One row per player-team; classifies as `homegrown`, `young_acq`, or `veteran_acq` |
#' | `LeagueMedianSalary` | `SalariesAll` | League-wide median and mean salary by season; use for relative-salary normalisation |
#' | `TeamPayroll` | `SalariesAll` | Total payroll, player count, median and max salary by team-season |
#'
#' **Scalar macro:**
#'
#' | Macro | Argument | Returns |
#' |-------|----------|---------|
#' | `era_label(yr)` | `INTEGER` year | `'Pre-Moneyball'` (1998-2002), `'Moneyball'` (2003-2011), `'Big Data'` (2012+), or `NULL` |
#'
#' Use `era_label(yearID)` in any SQL query instead of repeating the `CASE`
#' block. Example: `SELECT era_label(yearID) AS era, ... FROM BattingStats`.
#'
#' **Acquisition type** (`PlayerAcquisitionType.acq_type`):
#' - `homegrown` — player's first MLB season equals first season with this team
#' - `young_acq` — joined team after MLB debut, age on arrival < 26
#' - `veteran_acq` — joined team after MLB debut, age on arrival >= 26
#'
#' **FIP constant** is derived per `yearID + lgID` by aggregating the `Teams`
#' table (`lgERA - (13*lgHR + 3*lgBB - 2*lgSO) / lgIP`), so it correctly
#' adjusts for era and league scoring environment. Falls back to 3.10 only
#' for seasons with no matching `Teams` row.
#'
#' @param con A writable `DBIConnection` to the baseball DuckDB database.
#'
#' @return Invisibly returns `con`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db("baseball.duckdb", read_only = FALSE)
#' create_stats_views(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
create_stats_views <- function(con) {

  # ── BattingStats ─────────────────────────────────────────────────────────────
  # Columns of note:
  #   X2B, X3B  -- stored with R's prefix because "2B"/"3B" are invalid identifiers
  #   HBP, SF, SH -- nullable in early seasons; COALESCE to 0
  #   OBP denominator excludes SH (sacrifice bunts) per official MLB definition
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW BattingStats AS
    SELECT
      playerID, yearID, stint, teamID, lgID,
      G, AB, R, H, X2B, X3B, HR, RBI, SB, CS,
      BB, SO, IBB,
      COALESCE(HBP,  0) AS HBP,
      COALESCE(SH,   0) AS SH,
      COALESCE(SF,   0) AS SF,
      COALESCE(GIDP, 0) AS GIDP,

      -- Plate appearances
      (AB + BB
        + COALESCE(HBP, 0)
        + COALESCE(SF,  0)
        + COALESCE(SH,  0))                                         AS PA,

      -- Batting average
      H::DOUBLE / NULLIF(AB, 0)                                     AS AVG,

      -- On-base percentage  (H + BB + HBP) / (AB + BB + HBP + SF)
      (H + BB + COALESCE(HBP, 0))::DOUBLE
        / NULLIF(AB + BB + COALESCE(HBP, 0) + COALESCE(SF, 0), 0)  AS OBP,

      -- Slugging  (TB / AB),  TB = H + 2B + 2*3B + 3*HR
      (H + X2B + 2 * X3B + 3 * HR)::DOUBLE
        / NULLIF(AB, 0)                                             AS SLG,

      -- OPS = OBP + SLG
      (H + BB + COALESCE(HBP, 0))::DOUBLE
        / NULLIF(AB + BB + COALESCE(HBP, 0) + COALESCE(SF, 0), 0)
      + (H + X2B + 2 * X3B + 3 * HR)::DOUBLE
        / NULLIF(AB, 0)                                             AS OPS,

      -- Isolated power: extra bases per AB
      (X2B + 2 * X3B + 3 * HR)::DOUBLE
        / NULLIF(AB, 0)                                             AS ISO,

      -- BABIP  (H - HR) / (AB - SO - HR + SF)
      (H - HR)::DOUBLE
        / NULLIF(AB - SO - HR + COALESCE(SF, 0), 0)                AS BABIP,

      -- Walk and strikeout rates (per PA)
      BB::DOUBLE
        / NULLIF(AB + BB + COALESCE(HBP,0) + COALESCE(SF,0) + COALESCE(SH,0), 0)
                                                                    AS BB_pct,
      SO::DOUBLE
        / NULLIF(AB + BB + COALESCE(HBP,0) + COALESCE(SF,0) + COALESCE(SH,0), 0)
                                                                    AS K_pct
    FROM Batting
  ")
  message(sprintf("  %-25s (view)", "BattingStats"))

  # ── PitchingStats ────────────────────────────────────────────────────────────
  # IPouts = total outs recorded (IP * 3); use throughout to avoid /3 /3 chains.
  # ERA is already in the Lahman table; recomputed here for consistency and to
  # handle rows where the stored value may be NULL.
  # FIP constant is derived per yearID + lgID from the Teams table so it
  # adjusts for era and league scoring environment (Year of the Pitcher vs
  # juiced-ball seasons, AL DH era, etc.).  Falls back to 3.10 when Teams
  # has no matching row (pre-1871 edge cases).
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW PitchingStats AS
    WITH lg_stats AS (
      -- Aggregate to league level; Teams has one row per team per season
      SELECT
        yearID,
        lgID,
        SUM(IPouts)                    AS lg_IPouts,
        SUM(COALESCE(HRA, 0))          AS lg_HR,
        SUM(COALESCE(BBA, 0))          AS lg_BB,
        SUM(COALESCE(SOA, 0))          AS lg_SO,
        SUM(COALESCE(ER,  0))          AS lg_ER
      FROM Teams
      GROUP BY yearID, lgID
    ),
    fip_constants AS (
      -- FIP constant = lgERA - (13*lgHR + 3*lgBB - 2*lgSO) / lgIP
      SELECT
        yearID,
        lgID,
        lg_ER * 27.0 / NULLIF(lg_IPouts, 0)                        AS lg_ERA,
        lg_ER * 27.0 / NULLIF(lg_IPouts, 0)
          - (13.0 * lg_HR + 3.0 * lg_BB - 2.0 * lg_SO)
            / NULLIF(lg_IPouts / 3.0, 0)                            AS fip_constant
      FROM lg_stats
    )
    SELECT
      p.playerID, p.yearID, p.stint, p.teamID, p.lgID,
      p.W, p.L, p.G, p.GS, p.CG, p.SHO, p.SV, p.GF,
      p.IPouts,
      p.H, p.R, p.ER, p.HR, p.BB, p.SO,
      p.IBB, p.WP, p.HBP, p.BK, p.BFP,
      p.BAOpp, p.ERA,
      COALESCE(p.SH,   0) AS SH,
      COALESCE(p.SF,   0) AS SF,
      COALESCE(p.GIDP, 0) AS GIDP,

      -- Innings pitched (decimal)
      p.IPouts::DOUBLE / 3.0                                        AS IP,

      -- Win percentage
      p.W::DOUBLE / NULLIF(p.W + p.L, 0)                           AS Win_pct,

      -- WHIP  (BB + H) / IP
      (p.BB + p.H)::DOUBLE / NULLIF(p.IPouts, 0) * 3.0             AS WHIP,

      -- Per-9-inning rates
      p.SO  * 27.0 / NULLIF(p.IPouts, 0)                           AS K_9,
      p.BB  * 27.0 / NULLIF(p.IPouts, 0)                           AS BB_9,
      p.HR  * 27.0 / NULLIF(p.IPouts, 0)                           AS HR_9,
      p.H   * 27.0 / NULLIF(p.IPouts, 0)                           AS H_9,

      -- Strikeout-to-walk ratio
      p.SO::DOUBLE / NULLIF(p.BB, 0)                                AS K_BB,

      -- Era-adjusted FIP using per-year/league constant from Teams
      -- Falls back to 3.10 if no matching Teams row exists
      (13.0 * p.HR + 3.0 * (p.BB + COALESCE(p.HBP, 0)) - 2.0 * p.SO)
        / NULLIF(p.IPouts::DOUBLE / 3.0, 0)
        + COALESCE(fc.fip_constant, 3.10)                           AS FIP,

      -- Expose the constant so callers can see which era adjustment was applied
      COALESCE(fc.fip_constant, 3.10)                               AS FIP_constant
    FROM Pitching p
    LEFT JOIN fip_constants fc
      ON p.yearID = fc.yearID AND p.lgID = fc.lgID
  ")
  message(sprintf("  %-25s (view)", "PitchingStats"))

  # ── FieldingStats ────────────────────────────────────────────────────────────
  # InnOuts = outs the player was on the field (innings * 3).
  # RF/9 and FPCT are undefined for pitchers/DHs; NULLs propagate naturally.
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW FieldingStats AS
    SELECT
      playerID, yearID, stint, teamID, lgID, POS,
      G, GS, InnOuts, PO, A, E, DP,
      PB, WP, SB, CS, ZR,

      -- Fielding percentage
      (PO + A)::DOUBLE / NULLIF(PO + A + E, 0)                     AS FPCT,

      -- Range factor per 9 innings  (PO + A) * 27 / InnOuts
      (PO + A) * 27.0 / NULLIF(InnOuts, 0)                         AS RF_9,

      -- Range factor per game  (PO + A) / G
      (PO + A)::DOUBLE / NULLIF(G, 0)                               AS RF_G
    FROM Fielding
  ")
  message(sprintf("  %-25s (view)", "FieldingStats"))

  # ── era_label macro ──────────────────────────────────────────────────────────
  # Scalar macro so every analysis query can write era_label(yearID) rather than
  # repeating the same CASE block.  Returns NULL for years outside 1998-present.
  DBI::dbExecute(con, "
    CREATE OR REPLACE MACRO era_label(yr) AS
      CASE
        WHEN yr BETWEEN 1998 AND 2002 THEN 'Pre-Moneyball'
        WHEN yr BETWEEN 2003 AND 2011 THEN 'Moneyball'
        WHEN yr >= 2012              THEN 'Big Data'
        ELSE NULL
      END
  ")
  message(sprintf("  %-25s (macro)", "era_label(yr)"))

  # ── PlayerAcquisitionType ────────────────────────────────────────────────────
  # Classifies every player-team first appearance as homegrown, young_acq, or
  # veteran_acq by comparing the player's MLB debut year to their first year
  # with this specific team and their age on arrival.
  #   homegrown   — debut year == first year with this team
  #   young_acq   — arrived after debut, age on arrival < 26
  #   veteran_acq — arrived after debut, age on arrival >= 26
  # Note: "first MLB year" is determined from Batting + Pitching combined, so
  # pitchers who never batted are classified correctly.
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW PlayerAcquisitionType AS
    WITH all_apps AS (
      SELECT playerID, yearID, teamID FROM Batting
      UNION ALL
      SELECT playerID, yearID, teamID FROM Pitching
    ),
    mlb_debut AS (
      SELECT playerID, MIN(yearID) AS mlb_debut_year
      FROM all_apps
      GROUP BY playerID
    ),
    first_with_team AS (
      SELECT playerID, teamID, MIN(yearID) AS first_team_year
      FROM all_apps
      GROUP BY playerID, teamID
    )
    SELECT
      fwt.playerID,
      fwt.teamID,
      fwt.first_team_year,
      md.mlb_debut_year,
      pe.birthYear,
      fwt.first_team_year - pe.birthYear     AS age_on_arrival,
      CASE
        WHEN fwt.first_team_year = md.mlb_debut_year                    THEN 'homegrown'
        WHEN fwt.first_team_year > md.mlb_debut_year
             AND fwt.first_team_year - pe.birthYear < 26                THEN 'young_acq'
        ELSE                                                                 'veteran_acq'
      END                                    AS acq_type
    FROM first_with_team fwt
    JOIN mlb_debut       md  USING (playerID)
    JOIN People          pe  USING (playerID)
  ")
  message(sprintf("  %-25s (view)", "PlayerAcquisitionType"))

  # ── LeagueMedianSalary ───────────────────────────────────────────────────────
  # One row per season with league-wide salary distribution metrics.
  # Use for normalising individual salaries: salary / med_sal gives relative pay.
  # Requires SalariesAll to exist (created by setup_baseball_db()).
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW LeagueMedianSalary AS
    SELECT
      yearID,
      MEDIAN(salary)           AS med_sal,
      AVG(salary)              AS avg_sal,
      COUNT(DISTINCT playerID) AS n_players
    FROM SalariesAll
    WHERE is_actual = TRUE AND salary > 0
    GROUP BY yearID
  ")
  message(sprintf("  %-25s (view)", "LeagueMedianSalary"))

  # ── TeamPayroll ──────────────────────────────────────────────────────────────
  # Team-season level salary aggregates.  total_salary is the primary metric;
  # median_salary and max_salary support Gini and concentration analysis.
  # Requires SalariesAll to exist (created by setup_baseball_db()).
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW TeamPayroll AS
    SELECT
      yearID,
      teamID,
      SUM(salary)              AS total_salary,
      COUNT(DISTINCT playerID) AS n_players,
      MEDIAN(salary)           AS median_salary,
      MAX(salary)              AS max_salary
    FROM SalariesAll
    WHERE is_actual = TRUE AND salary > 0
    GROUP BY yearID, teamID
  ")
  message(sprintf("  %-25s (view)", "TeamPayroll"))

  invisible(con)
}
