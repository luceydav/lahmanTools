# lahmanTools (development version)

## New features

* `write_mcp_config()` -- generates the JSON config entry needed to connect
  GitHub Copilot CLI or Claude Code to `baseball.duckdb` via a local DuckDB
  MCP server. Resolves `~` to an absolute path (required by Python-based MCP
  servers), merges into an existing config without clobbering other server
  entries, and always enforces `--readonly`. Defaults to `dry_run = TRUE` so
  nothing is written until the user opts in.

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
