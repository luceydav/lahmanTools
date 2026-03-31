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
#' | `PlayoffPayroll` | `SeriesPost`, `TeamPayroll`, `Teams` | Team payroll for each playoff round reached; `rounds_won` counts series wins |
#' | `AllStarConcentration` | `AllstarFull`, `TeamPayroll` | All-Star selections per team-season with payroll; supports talent-per-dollar analysis |
#' | `AwardSalaryPremium` | `AwardsPlayers`, `SalariesAll`, `PlayerWAR` | Salary and WAR in the year before and year of key awards (MVP, Cy Young, Gold Glove, ROY) |
#' | `HOFCareerArc` | `HallOfFame`, `SalaryPerWAR`, `PlayerWAR` | Inducted players with career WAR and salary arc; one row per player-year |
#' | `PositionalPayroll` | `Appearances`, `SalariesAll`, `PlayerWAR` | Salary, WAR, and WAR/salary by primary position and era |
#' | `ManagerPerformance` | `Managers`, `Teams`, `TeamPayroll` | Manager W-L%, finish rank, and payroll per managed season |
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

  # ── PlayoffPayroll ───────────────────────────────────────────────────────────
  # One row per team-year-round combination showing payroll at time of playoff
  # run.  rounds_won counts series wins for that team in that year.
  # Useful for: does payroll correlate with playoff depth/championships?
  # round codes: WS = World Series, ALCS/NLCS, ALDS1/ALDS2/NLDS1/NLDS2,
  #              ALWC/NLWC = Wild Card game
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW PlayoffPayroll AS
    WITH series AS (
      SELECT yearID, round,
             teamIDwinner AS teamID, 1 AS is_winner
      FROM SeriesPost
      UNION ALL
      SELECT yearID, round,
             teamIDloser  AS teamID, 0 AS is_winner
      FROM SeriesPost
    ),
    team_rounds AS (
      SELECT yearID, teamID,
             COUNT(DISTINCT round) AS rounds_played,
             SUM(is_winner)        AS rounds_won,
             MAX(CASE WHEN round = 'WS' THEN is_winner END) AS won_ws
      FROM series
      GROUP BY yearID, teamID
    )
    SELECT
      tr.yearID,
      tr.teamID,
      t.name AS team_name,
      tp.total_salary,
      tr.rounds_played,
      tr.rounds_won,
      COALESCE(tr.won_ws, 0) AS won_ws,
      era_label(tr.yearID)   AS era
    FROM team_rounds tr
    LEFT JOIN TeamPayroll tp USING (yearID, teamID)
    LEFT JOIN Teams       t  USING (yearID, teamID)
    WHERE tp.total_salary IS NOT NULL
  ")
  message(sprintf("  %-25s (view)", "PlayoffPayroll"))

  # ── AllStarConcentration ─────────────────────────────────────────────────────
  # One row per team-year: number of All-Stars selected and payroll.
  # n_allstar_starts counts players who started the game (startingPos not NULL).
  # Useful for: is spending concentrated in stars or spread across depth?
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW AllStarConcentration AS
    SELECT
      a.yearID,
      a.teamID,
      t.name                        AS team_name,
      COUNT(DISTINCT a.playerID)    AS n_allstars,
      SUM(CASE WHEN a.startingPos IS NOT NULL THEN 1 ELSE 0 END)
                                    AS n_allstar_starts,
      tp.total_salary,
      tp.n_players,
      COUNT(DISTINCT a.playerID)::DOUBLE
        / NULLIF(tp.n_players, 0)   AS allstar_rate,
      era_label(a.yearID)           AS era
    FROM AllstarFull a
    LEFT JOIN TeamPayroll tp USING (yearID, teamID)
    LEFT JOIN Teams       t  USING (yearID, teamID)
    GROUP BY a.yearID, a.teamID, t.name, tp.total_salary, tp.n_players
  ")
  message(sprintf("  %-25s (view)", "AllStarConcentration"))

  # ── AwardSalaryPremium ───────────────────────────────────────────────────────
  # Only created when PlayerWAR exists (requires load_war = TRUE).
  player_war_exists <- "PlayerWAR" %in% DBI::dbListTables(con)
  if (player_war_exists) {
    DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW AwardSalaryPremium AS
      WITH key_awards AS (
        SELECT playerID, yearID, awardID
        FROM AwardsPlayers
        WHERE awardID IN (
          'Most Valuable Player', 'Cy Young Award',
          'Gold Glove', 'Rookie of the Year'
        )
      ),
      sal AS (
        SELECT playerID, yearID, salary
        FROM SalariesAll
        WHERE is_actual = TRUE AND salary > 0
      ),
      war AS (
        SELECT playerID, yearID,
               bWAR + pWAR AS total_war
        FROM PlayerWAR
      )
      SELECT
        ka.playerID,
        p.nameFirst || ' ' || p.nameLast AS player_name,
        ka.yearID,
        ka.awardID,
        s0.salary            AS salary_award_yr,
        s1.salary            AS salary_next_yr,
        s1.salary - s0.salary AS salary_delta,
        w0.total_war         AS war_award_yr,
        w1.total_war         AS war_next_yr,
        era_label(ka.yearID) AS era
      FROM key_awards ka
      JOIN People p          USING (playerID)
      LEFT JOIN sal s0       ON s0.playerID = ka.playerID AND s0.yearID = ka.yearID
      LEFT JOIN sal s1       ON s1.playerID = ka.playerID AND s1.yearID = ka.yearID + 1
      LEFT JOIN war w0       ON w0.playerID = ka.playerID AND w0.yearID = ka.yearID
      LEFT JOIN war w1       ON w1.playerID = ka.playerID AND w1.yearID = ka.yearID + 1
      WHERE s0.salary IS NOT NULL
    ")
    message(sprintf("  %-25s (view)", "AwardSalaryPremium"))
  } else {
    message(sprintf("  %-25s (skipped -- PlayerWAR not loaded)", "AwardSalaryPremium"))
  }

  # ── HOFCareerArc ─────────────────────────────────────────────────────────────
  # Only created when PlayerWAR exists (requires load_war = TRUE).
  if (player_war_exists) {
    DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW HOFCareerArc AS
      WITH hof_inducted AS (
        SELECT playerID, MIN(yearID) AS inducted_year
        FROM HallOfFame
        WHERE inducted = 'Y' AND category = 'Player'
        GROUP BY playerID
      ),
      war AS (
        SELECT playerID, yearID,
               bWAR + pWAR AS total_war
        FROM PlayerWAR
      )
      SELECT
        hi.playerID,
        p.nameFirst || ' ' || p.nameLast AS player_name,
        hi.inducted_year,
        w.yearID,
        hi.inducted_year - w.yearID      AS years_before_induction,
        w.total_war,
        s.salary,
        s.salary::DOUBLE / NULLIF(w.total_war, 0) AS salary_per_war,
        era_label(w.yearID)              AS era
      FROM hof_inducted hi
      JOIN People p        USING (playerID)
      LEFT JOIN war w      USING (playerID)
      LEFT JOIN SalariesAll s
        ON s.playerID = hi.playerID
       AND s.yearID   = w.yearID
       AND s.is_actual = TRUE
      WHERE w.yearID IS NOT NULL
    ")
    message(sprintf("  %-25s (view)", "HOFCareerArc"))
  } else {
    message(sprintf("  %-25s (skipped -- PlayerWAR not loaded)", "HOFCareerArc"))
  }

  # ── PositionalPayroll ────────────────────────────────────────────────────────
  # Only created when PlayerWAR exists (requires load_war = TRUE).
  if (player_war_exists) {
    DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW PositionalPayroll AS
      WITH pos_games AS (
        SELECT playerID, yearID,
          UNNEST(['P','C','1B','2B','3B','SS','LF','CF','RF','DH']) AS pos,
          UNNEST([G_p, G_c, G_1b, G_2b, G_3b, G_ss, G_lf, G_cf, G_rf, G_dh]) AS g_pos
        FROM Appearances
      ),
      primary_pos AS (
        SELECT playerID, yearID, pos AS primary_pos
        FROM (
          SELECT playerID, yearID, pos,
                 ROW_NUMBER() OVER (
                   PARTITION BY playerID, yearID
                   ORDER BY g_pos DESC
                 ) AS rn
          FROM pos_games
          WHERE g_pos > 0
        )
        WHERE rn = 1
      ),
      war AS (
        SELECT playerID, yearID, bWAR + pWAR AS total_war
        FROM PlayerWAR
      )
      SELECT
        pp.playerID,
        pp.yearID,
        pp.primary_pos,
        s.teamID,
        s.salary,
        w.total_war,
        s.salary::DOUBLE / NULLIF(w.total_war, 0) AS salary_per_war,
        era_label(pp.yearID)                        AS era
      FROM primary_pos pp
      LEFT JOIN SalariesAll s
        ON s.playerID = pp.playerID
       AND s.yearID   = pp.yearID
       AND s.is_actual = TRUE
      LEFT JOIN war w
        ON w.playerID = pp.playerID
       AND w.yearID   = pp.yearID
      WHERE s.salary IS NOT NULL AND s.salary > 0
    ")
    message(sprintf("  %-25s (view)", "PositionalPayroll"))
  } else {
    message(sprintf("  %-25s (skipped -- PlayerWAR not loaded)", "PositionalPayroll"))
  }

  # ── ManagerPerformance ───────────────────────────────────────────────────────
  # One row per manager-team-season (matching Managers table grain).
  # win_pct = W / (W + L); rank is division finish (1 = first place).
  # payroll is the TeamPayroll for that year so you can ask whether well-paid
  # teams are managed more efficiently.
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW ManagerPerformance AS
    SELECT
      m.playerID,
      p.nameFirst || ' ' || p.nameLast AS manager_name,
      m.yearID,
      m.teamID,
      t.name                            AS team_name,
      m.G,
      m.W,
      m.L,
      m.W::DOUBLE / NULLIF(m.G, 0)     AS win_pct,
      m.rank,
      tp.total_salary,
      era_label(m.yearID)               AS era
    FROM Managers m
    JOIN People p        USING (playerID)
    LEFT JOIN Teams t    USING (yearID, teamID)
    LEFT JOIN TeamPayroll tp USING (yearID, teamID)
    WHERE m.inseason = 1
  ")
  message(sprintf("  %-25s (view)", "ManagerPerformance"))

  invisible(con)
}
