# Copilot Instructions

## Project Overview

`lahmanTools` is an R package that loads all [Lahman](https://cran.r-project.org/package=Lahman) baseball tables plus scraped USA Today salary data into a persistent **file-backed DuckDB** (`baseball.duckdb`). Analysis is done via SQL views and `data.table` — no tidyverse.

## Package Structure

```
R/
  connect.R      # connect_baseball_db()    -- open file-backed DuckDB
  setup_db.R     # setup_baseball_db()      -- build/rebuild baseball.duckdb
  stats_views.R  # create_stats_views()     -- add BattingStats/PitchingStats/FieldingStats views
  scrape.R       # scrape_salaries()        -- scrape databases.usatoday.com
  utils.R        # dt_factors_to_char(), clean_names(), db_query() -- shared helpers
  globals.R      # globalVariables() + @importFrom tags
data-raw/        # archived original scripts (not part of package build)
```

## Data Sources

- **`Lahman` R package** — primary source through 2016. Key tables: `People`, `Batting`, `Pitching`, `Fielding`, `Teams`, `Salaries`. Metadata tables (`LahmanData`, `battingLabels`, etc.) are skipped when loading.
- **`inst/extdata/mlb_salaries/salaries_2017_2024_with_playerID.csv`** — scraped salary data (2022-2025) from `databases.usatoday.com`, matched to Lahman `playerID`.
- **`baseball.duckdb`** — the persistent file-backed database. **Never committed to git.** Rebuild with `setup_baseball_db()`.

## Views in baseball.duckdb

| View | Base table(s) | Key additions |
|------|--------------|---------------|
| `SalariesAll` | `Salaries` + `SalariesUSAToday` | Unions Lahman (<=2016) with USA Today (2022-2025); imputes missing contract years using AAV straight-lining via `generate_series` |
| `BattingStats` | `Batting` | PA, AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K% |
| `PitchingStats` | `Pitching` + `Teams` | IP, Win%, WHIP, K/9, BB/9, HR/9, H/9, K/BB, FIP (era-adjusted), FIP_constant |
| `FieldingStats` | `Fielding` | FPCT, RF/9, RF/G |

FIP constant is computed per `yearID + lgID` from the `Teams` table: `lgERA - (13*lgHR + 3*lgBB - 2*lgSO) / lgIP`. Falls back to 3.10 only for pre-1871 edge cases.

## Standard Session Pattern

```r
library(lahmans)

# Read-only analysis session (default)
con <- connect_baseball_db()            # opens baseball.duckdb read_only = TRUE
on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

# Query a view
DBI::dbGetQuery(con, "SELECT * FROM BattingStats WHERE yearID = 2023 ORDER BY OPS DESC LIMIT 20")

# Rebuild the whole database (write access, only when rebuilding)
setup_baseball_db("baseball.duckdb", overwrite = TRUE)

# Add/refresh only the stats views (writable connection)
con_rw <- connect_baseball_db(read_only = FALSE)
create_stats_views(con_rw)
DBI::dbDisconnect(con_rw, shutdown = TRUE)
```

> **Note:** RStudio may lock `baseball.duckdb`. Use `read_only = TRUE` for analysis when RStudio has the file open.

## Key Packages

| Package | Purpose |
|---------|---------|
| `duckdb` | File-backed SQL engine -- preferred for aggregations and joins |
| `DBI` | Database interface |
| `Lahman` | Baseball historical data (used at build time in `setup_db.R`) |
| `data.table` | All in-R data manipulation; never dplyr |
| `httr2`, `rvest`, `xml2` | Web scraping in `scrape.R` |
| `dm` | (suggested) Database model introspection via `dm_from_con()` |
| `re2` | (suggested) Vectorized regex, faster than base `gsub`/`grepl` |
| `ggplot2` | (suggested) Plotting |

## Code Style

**Prefer DuckDB SQL over R loops for aggregation.** Use R (data.table) for post-query reshaping only.

**Avoid tidyverse entirely** (`dplyr`, `tidyr`, `purrr`, `readr`, `stringr`, `janitor`). Use base R or data.table:

| Tidyverse | Prefer instead |
|-----------|---------------|
| `dplyr::mutate` / `filter` / `select` | `dt[, col := ...]` / `dt[cond]` / `dt[, .(col)]` |
| `dplyr::left_join` | `merge(..., all.x = TRUE)` or `dt1[dt2, on = ...]` |
| `tidyr::pivot_wider` / `pivot_longer` | `dcast(dt, ...)` / `melt(dt, ...)` |
| `purrr::map` / `map_dfr` | `lapply` / `rbindlist(lapply(...))` |
| `readr::read_csv` / `write_csv` | `fread()` / `fwrite()` |
| `stringr::str_replace_all` | `gsub()` or `re2::re2_replace_all()` |
| `janitor::clean_names` | `setnames(dt, tolower(gsub("[^a-z0-9]+", "_", names(dt))))` |

Use `fread()`/`fwrite()` for all CSV I/O -- never `read.csv`/`write.csv`.

## data.table Patterns and Gotchas

### Core idioms
```r
# Modify in place -- no reassignment needed
dt[, full_name := paste0(nameLast, ", ", nameFirst)]

# Grouped aggregation
dt[, .(avg = mean(salary, na.rm = TRUE)), by = .(yearID, teamID)]

# Chained operations
dt[yearID >= 2000][order(-salary)][, head(.SD, 10)]

# Fast join (set keys first for large tables)
setkey(batting, playerID)
setkey(people,  playerID)
batting[people, on = "playerID", nomatch = 0L]
```

### Gotchas
- **`DT[, col]` returns a vector; `DT[, .(col)]` returns a data.table.** Use `.(...)` when you need a table back.
- **`:=` modifies by reference.** Use `copy(dt)` before mutating if the original must be preserved.
- **`setDT()` converts a data.frame in place** (no copy); use `as.data.table()` when you need a copy.
- **`.SD` with `lapply`** for multi-column ops: `dt[, lapply(.SD, sum, na.rm = TRUE), by = yearID, .SDcols = c("HR", "SO", "BB")]`.
- **Avoid `1:nrow(dt)`** -- use `seq_len(nrow(dt))` to guard against zero-row tables.
- **Never use `T`/`F`** as boolean literals; always write `TRUE`/`FALSE`.
- **Integer overflow** -- Lahman count columns (`AB`, `H`, `SO`, etc.) are integers; cast before summing large groups: `sum(as.numeric(AB))`.
- **`importFrom(data.table, "unique")` fails** -- `unique` is an S3 method; use `base::unique()` which dispatches correctly to data.table.

## DuckDB Patterns and Gotchas

- **Always `shutdown = TRUE`** in `dbDisconnect()` -- omitting it leaves the DuckDB process running.
- **File lock** -- only one writable connection at a time. Multiple readers are fine with `read_only = TRUE`.
- **`duckdb_register()` does not copy data** -- do not modify the registered data.table in place while the connection is open.
- **Use `SUMMARIZE <table>`** for quick profiling instead of pulling data into R.
- **Float division** -- DuckDB integer columns require explicit casting: `sum(SO)::DOUBLE / sum(AB)` (not `* 1.0`).
- **`DISTINCT ON (cols) ORDER BY`** -- DuckDB idiom for deduplication keeping a preferred row.
- **`generate_series()` with `LATERAL`** -- used in `SalariesAll` to expand contract year ranges.
- **In `.qmd` files**, SQL chunks use `#| connection: con`; in `.Rmd`, use `{sql connection=con}`.

## Lahman-Specific Notes

- **Column names** -- `2B`/`3B` are stored as `X2B`/`X3B` in DuckDB (R renames invalid identifiers).
- **`Salaries` table only goes through 2016** -- use `SalariesAll` view for post-2016 seasons.
- **`Teams` table goes through 2025** -- used for era-adjusted FIP constants in `PitchingStats`.
- **`IPouts`** = total outs recorded (IP * 3). `InnOuts` in `Fielding` is the same concept for fielders.
- **Lahman salaries are actual year-by-year** (not AAV) -- confirmed via A-Rod/Pujols multi-year contracts.
- **Skip on load**: `LahmanData`, `battingLabels`, `fieldingLabels`, `pitchingLabels` (metadata, not data).

## SalariesAll View Logic

`SalariesAll` bridges two salary sources:

1. **Lahman** (`source = 'lahman'`, `is_actual = TRUE`) -- all rows through 2016.
2. **USA Today** (`source = 'usatoday'`) -- actual rows (`is_actual = TRUE`) plus AAV-imputed rows for contract years not scraped (`is_actual = FALSE`).

Contract years are extracted via regex from the `years` column (patterns: `"N (YYYY-YY)"`, `"N(YYYY-YY)"`). Missing contract years within the range are filled with `average_annual` (AAV). Rows with NULL `years` (~78% -- one-year deals) pass through as actual records.

Filter to `is_actual = TRUE` for real salary figures. Include `is_actual = FALSE` to fill gaps in long-term contracts.

## Salary Scraper Notes

`scrape_salaries()` in `R/scrape.R`:
- Scrapes `databases.usatoday.com` one record at a time (IDs 1-1000 per year).
- Year slugs differ: `"major-league-baseball-salaries-2023"` vs `"mlb-salaries-2017"` (pre-2023).
- Rate-limited via `httr2::req_throttle()`. Skips years where output CSV already exists.
- Outputs per-year CSVs to `inst/extdata/mlb_salaries/`, then combines with `playerID` join.
- Unmatched players written to `inst/extdata/mlb_salaries/unmatched_players.csv` for manual review.

## SQL Style

- Column names follow Lahman camelCase: `playerID`, `yearID`, `teamID`, `franchID`.
- Active franchises filter: `WHERE franchID IN (SELECT DISTINCT franchID FROM TeamsfranchISES WHERE active = 'Y')`.
- Use `COALESCE(col, 0)` not `IFNULL` for nullable Lahman columns (`HBP`, `SF`, `SH`, `GIDP`).

## Conventions

### playerID Joining
`playerID` is the canonical key across all tables. To link external name-based data:
```r
people <- data.table::as.data.table(Lahman::People)
people[, player := paste0(nameLast, ", ", nameFirst)]
sal_linked <- merge(scraped_dt, people[, .(playerID, player)],
                    by = "player", all.x = TRUE)
sal_linked[is.na(playerID), .(player)]   # inspect unmatched
```

### R CMD check
- Non-ASCII chars (em-dashes, box-drawing) in R source files cause WARNING -- use ASCII `--`.
- `VignetteBuilder: knitr` in DESCRIPTION without actual vignettes causes NOTE -- omit it.
- `Depends: R (>= 4.1.0)` required when using native pipe `|>`.
- `utils::globalVariables()` in `globals.R` silences CMD check NOTEs for data.table NSE columns.
