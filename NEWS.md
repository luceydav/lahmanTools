# lahmanTools 0.2.0

## New features

* Three new runtime data loaders in `R/loaders.R`:
  - `load_chadwick_ids(con)` -- downloads the Chadwick Bureau player ID
    crosswalk via `baseballr` and writes it as `ChadwickIDs` to DuckDB.
    Creates `PlayerIDs` view joining Lahman `playerID` to MLBAM, FanGraphs,
    Retrosheet and Baseball Reference IDs. Licensed ODC-BY 1.0 (attribution
    required).
  - `load_fangraphs_war(con, years)` -- fetches FanGraphs batter and pitcher
    WAR leaderboards (batting 1871+, pitching 1985+) and creates `PlayerWAR`
    and `SalaryPerWAR` views. Requires `ChadwickIDs` for the FanGraphs-to-Lahman
    join. `SalaryPerWAR` includes a `war_reliable` flag (TRUE for all rows in
    the salary era 1985+; retained for backward compatibility).
  - `load_statcast(con, years)` -- fetches Baseball Savant pitch-level data
    (2015+ only, ~700 MB/season) and creates `StatcastSeason` batter aggregates
    (exit velocity, launch angle, hard-hit rate, xBA, xwOBA).

* `setup_baseball_db()` gains three new parameters:
  - `load_chadwick = FALSE` -- pass `TRUE` to load the Chadwick crosswalk
    during initial database build.
  - `load_war = FALSE` -- pass `TRUE` to also fetch FanGraphs WAR (implies
    `load_chadwick`).
  - `war_years = 1985:2025` -- seasons to fetch for WAR data.

* `baseballr` added to `Suggests`; required only by the three new loaders.

* `write_mcp_config()` -- generates the JSON config entry needed to connect
  GitHub Copilot CLI or Claude Code to `baseball.duckdb` via a local DuckDB
  MCP server. Resolves `~` to an absolute path (required by Python-based MCP
  servers), merges into an existing config without clobbering other server
  entries, and always enforces `--readonly`. Defaults to `dry_run = TRUE` so
  nothing is written until the user opts in.

* Three new analytical views created by `create_stats_views()` / `setup_baseball_db()`:
  - `PlayerAcquisitionType` -- one row per player-team; `acq_type` column
    classifies as `homegrown` (debut year = first year with team),
    `young_acq` (arrived post-debut, age < 26), or `veteran_acq`.
    Eliminates the repeated 3-CTE acquisition-classification pattern in
    analysis queries.
  - `LeagueMedianSalary` -- `med_sal`, `avg_sal`, `n_players` by season from
    `SalariesAll`. Use `salary / med_sal` for relative-salary normalisation.
  - `TeamPayroll` -- `total_salary`, `n_players`, `median_salary`, `max_salary`
    by team-season from `SalariesAll`. Was documented in README but missing
    from the code; now implemented.

* `era_label(yr)` SQL macro registered by `create_stats_views()`. Replaces
  the repeated `CASE WHEN yearID <= 2002 THEN 'Pre-Moneyball' ...` block in
  every analysis query. Returns `'Pre-Moneyball'`, `'Moneyball'`, `'Big Data'`,
  or `NULL` for years outside 1998-present.

# lahmanTools 0.1.0

Initial release.

## New features

* `setup_baseball_db()` — builds a persistent DuckDB file from all 27 Lahman
  tables. Optionally ingests USA Today scraped salary data via `SalariesUSAToday`
  and creates a unified `SalariesAll` view with AAV imputation for multi-year
  contracts.
* `connect_baseball_db()` — opens a DuckDB connection (read-only by default).
  Path is configurable via the `LAHMANS_DBDIR` environment variable.
* `create_stats_views()` — registers five sabermetric SQL views: `BattingStats`,
  `PitchingStats`, `FieldingStats`, `SalariesAll`, and `TeamPayroll`. FIP
  constant is derived per season and league from the `Teams` table.
* `scrape_salaries()` — fetches USA Today MLB salary pages with polite rate
  limiting; matches players to Lahman `playerID` via fuzzy name matching.
* `db_query()` — thin wrapper around `DBI::dbGetQuery()` returning a
  `data.table`. `dt_factors_to_char()` and `clean_names()` are also exported
  as utilities.
