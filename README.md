# lahmanTools

An R package that loads all [Lahman](https://cran.r-project.org/package=Lahman) baseball tables (1871–2025) plus USA Today salary data into a persistent **file-backed DuckDB** database. Analysis is done via SQL views and `data.table` — no tidyverse.

## Attribution

Baseball data provided by [Sean Lahman](http://www.seanlahman.com/) via the `Lahman` R package, licensed under [Creative Commons Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)](https://creativecommons.org/licenses/by-sa/3.0/). Any derivative work using Lahman data must carry the same attribution and license.

USA Today salary data is not bundled with this package. Users must obtain it independently by running `scrape_salaries()`.

## Installation

```r
# install.packages("pak")
pak::pak("your-github-username/lahmanTools")

# Also requires the Lahman data package
install.packages("Lahman")
```

## Setup

The database is stored outside the package, defaulting to `~/Documents/Data/baseball/baseball.duckdb`. Override the location by setting the `LAHMANS_DBDIR` environment variable (e.g. in `~/.Renviron`):

```sh
LAHMANS_DBDIR=/path/to/your/baseball.duckdb
```

Build the database once:

```r
library(lahmanTools)

# Uses LAHMANS_DBDIR or the default ~/Documents/Data/baseball/baseball.duckdb
setup_baseball_db()
```

To include USA Today salary data (2017–2025), scrape it first — **do not redistribute the resulting files**:

```r
# Scrape to ~/Documents/Data/baseball/mlb_salaries/ (or any directory)
scrape_salaries(years = 2017:2025,
                output_dir = "~/Documents/Data/baseball/mlb_salaries")

# Then rebuild with the salary file
setup_baseball_db(
  sal_file = "~/Documents/Data/baseball/mlb_salaries/salaries_2017_2025_with_playerID.csv",
  overwrite = TRUE
)
```

## Usage

```r
library(lahmanTools)

# Read-only analysis session (default)
con <- connect_baseball_db()
on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

# Use the helper to return a data.table directly
db_query(con, "SELECT yearID, AVG(OPS) FROM BattingStats
               WHERE yearID >= 2000 GROUP BY yearID ORDER BY yearID")

# Available views
# BattingStats  — PA, AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K%
# PitchingStats — IP, WHIP, K/9, BB/9, FIP (era-adjusted), K/BB
# FieldingStats — FPCT, RF/9, RF/G
DBI::dbListTables(con)
```

## Package structure

```
R/
  connect.R      # connect_baseball_db()    -- open file-backed DuckDB
  setup_db.R     # setup_baseball_db()      -- build/rebuild baseball.duckdb
  stats_views.R  # create_stats_views()     -- add derived stats views
  scrape.R       # scrape_salaries()        -- scrape databases.usatoday.com
  utils.R        # dt_factors_to_char(), clean_names(), db_query()
  globals.R      # globalVariables() + importFrom declarations
```

## Views

| View | Base tables | Key additions |
|------|------------|---------------|
| `BattingStats` | `Batting` | PA, AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K% |
| `PitchingStats` | `Pitching` + `Teams` | IP, WHIP, K/9, BB/9, HR/9, FIP (era-adjusted) |
| `FieldingStats` | `Fielding` | FPCT, RF/9, RF/G |
| `SalariesAll` | `Salaries` + `SalariesUSAToday` | Union of Lahman (≤2016) + USA Today (2017+); imputes missing contract years from AAV |

## License

MIT © David Lucey

Baseball data: CC BY-SA 3.0 — Sean Lahman
