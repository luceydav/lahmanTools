# lahmanTools 0.4.0

## Attribution fix

* **README** and **DESCRIPTION** now include Retrosheet as a credited data
  source.  `load_retrosheet_post()` has always carried the required attribution
  in its roxygen docs and `inst/RETROSHEET_NOTICE`; the top-level docs now
  match.

## Code quality

* Three call sites that bypassed `db_query()` now use it consistently:
  `scrape.R` (people lookup) and `utils.R` (Batting/Pitching roster tables in
  `match_player_ids()` Pass 4).  No behaviour change -- purely DRY cleanup.

# lahmanTools 0.3.0

## Breaking changes

* `setup_baseball_db()` no longer depends on the **Lahman** R package.
  All 27 tables are now loaded directly from the
  [cbwinslow/baseballdatabank](https://github.com/cbwinslow/baseballdatabank)
  CSV repository via DuckDB's `httpfs` extension. An internet connection is
  required the first time `setup_baseball_db()` is called.

* `scrape_salaries()` now requires a DuckDB `con=` argument and errors clearly
  if it is missing. The `Lahman::People` fallback path has been deleted.

* `match_player_ids()` now requires a DuckDB `con=` argument and errors clearly
  if it is missing. The `Lahman::Batting` / `Lahman::Pitching` fallback paths
  have been deleted.

## Dependency changes

* `Lahman` removed from `Suggests` entirely -- the package is no longer used
  anywhere.

* Core `Imports` are now `DBI`, `duckdb`, `data.table`, `httr2`, `rvest`,
  `xml2`. Indirect tidyverse dependencies (`dplyr`, `tibble`, `generics`,
  `tidyselect`) removed.

## New views in `create_stats_views()`

Six additional analytical views are now created by `create_stats_views()`:

* `PlayoffPayroll` -- team payroll for each playoff round; `rounds_won` and
  `won_ws` flag; supports payroll-to-championship analysis.
* `AllStarConcentration` -- All-Star selections per team-season with payroll;
  `allstar_rate` = All-Stars / roster size; use for star concentration vs.
  depth comparisons.
* `AwardSalaryPremium` -- salary and WAR in the award year and following year
  for MVP, Cy Young, Gold Glove, and Rookie of the Year winners; quantifies
  the contract premium following major awards. Requires `PlayerWAR` (i.e.,
  `setup_baseball_db(load_war = TRUE)`).
* `HOFCareerArc` -- inducted Hall of Fame players with season-level WAR and
  salary; `years_before_induction` aligns careers for peak-vs-pay analysis.
  Requires `PlayerWAR`.
* `PositionalPayroll` -- salary, WAR, and salary/WAR by primary position
  (derived from `Appearances`) and era; reveals which positions are
  systematically over- or under-paid. Requires `PlayerWAR`.
* `ManagerPerformance` -- manager W-L%, division finish rank, and team payroll
  per season; supports payroll efficiency vs. manager analysis.

## Bug fixes

* Fixed circular CTE reference in `SalariesAll` USA Today contract parsing
  (`usa_parsed2` was incorrectly self-referencing; corrected to reference
  `usa_parsed`).
* Fixed `bat_war` / `pit_war` column names in WAR-dependent views (were
  incorrectly using `bWAR` / `pWAR`).
* `match_player_ids()` now emits a `warning()` instead of silently returning
  `NULL` when `con=NULL` is passed.

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
