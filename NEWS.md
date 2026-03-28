# lahmanTools (development version)

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
