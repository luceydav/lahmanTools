# Copilot Instructions

<!-- Mirrors .github/copilot-instructions.md for cross-tool AI agent coverage (Claude Code, Cursor, Codex CLI).
     Keep both files in sync when updating. -->

## Project

`lahmanTools` is an R package for baseball sabermetric analysis. Stack: **DuckDB** (SQL engine) + **data.table** (R manipulation) + **base R** — no tidyverse. Analysis scripts live in `analysis/` (gitignored). `baseball.duckdb` is never committed; rebuild with `setup_baseball_db()`.

## DuckDB Schema

Database at `$LAHMANS_DBDIR/baseball.duckdb`. CLI access: `duckdb $LAHMANS_DBDIR/baseball.duckdb`.
Introspect: `SHOW TABLES`, `DESCRIBE <tbl>`, `SUMMARIZE <tbl>`, `dm::dm_from_con(con)`.

**Core Lahman tables** (all loaded at build time): `People`, `Batting`, `Pitching`, `Fielding`, `Teams`, `Salaries`, `Managers` + others. `playerID` is the canonical join key across all tables.

**Views:**

| View | Description |
|------|-------------|
| `SalariesAll` | Three-source union: Lahman (1985–2016) + Spotrac (2017–2021) + USA Today (2022–2025). Filter `is_actual = TRUE` for real figures. **Always use this, never query `Salaries` directly for multi-era work.** |
| `BattingStats` | Batting with PA, AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K% |
| `PitchingStats` | Pitching with IP, WHIP, K/9, BB/9, FIP (era-adjusted via `Teams`) |
| `FieldingStats` | Fielding with FPCT, RF/9, RF/G |

**Era definitions** (used in analysis queries):
- Pre-Moneyball: 1998–2002 | Moneyball: 2003–2011 | Big Data: 2012–present
- Exclude 2020 (60-game season): add `AND yearID != 2020`

**Extending the schema:** open a write connection → `DBI::dbWriteTable()` or `DBI::dbExecute("CREATE VIEW ...")` → `DBI::dbDisconnect(con, shutdown = TRUE)`. See `create_stats_views()` as the model.

## R Gotchas

- **`seq_along(x)` not `1:length(x)`** — when `x` is NULL/empty, `1:length(x)` produces `c(1, 0)` and iterates twice. Same hazard with `1:nrow(dt)` on zero-row tables; use `seq_len(nrow(dt))`.
- **NA propagates silently** — `sum(x)` returns `NA` if any element is `NA`; always pass `na.rm = TRUE`. Use `is.finite(x)` as the universal finite-value check (excludes `NA`, `NaN`, and `Inf`).
- **Integer overflow on Lahman counts** — `AB`, `H`, `SO`, etc. are stored as `integer`; cast before large group sums: `sum(as.numeric(AB))`.
- **`[[` vs `[` for lists** — `list[[1]]` extracts the element; `list[1]` returns a length-1 list.
- **`T`/`F` are not reserved** — they can be overwritten; always write `TRUE`/`FALSE`.
- **`drop = FALSE` on matrix/single-column subsetting** — `X[, 1]` silently drops the matrix class to a vector; use `X[, 1, drop = FALSE]` to preserve dimensions.
- **Factors bite silently** — assigning a value outside existing levels produces `NA` with only a warning. Prefer character columns; use `fread()` which defaults to character.

## data.table

- **`:=` modifies by reference** — no reassignment needed, but use `copy(dt)` before mutating if the original must be preserved.
- **`DT[, col]` returns a vector; `DT[, .(col)]` returns a data.table** — use `.(...)` when the result feeds another `data.table` operation.
- **`setkey()` enables fast binary-search subsetting** — set keys on large tables before repeated joins: `setkey(batting, playerID)`.
- **`.SDcols` for multi-column ops** — `dt[, lapply(.SD, sum, na.rm = TRUE), by = yearID, .SDcols = c("HR", "SO", "BB")]`.
- **`as.data.table(list(...))` not `data.table(list(...))`** — the latter wraps the entire list as a single column; the former creates one column per list element. Critical in tests.
- **`importFrom(data.table, "unique")` fails** — `unique` is an S3 generic; use `base::unique()` which dispatches to data.table correctly.

## DuckDB

- **Friendly SQL features worth using:** `SUMMARIZE <tbl>` for quick profiling; `GROUP BY ALL` to omit explicit group-by columns; lateral column aliases (reuse `SELECT` aliases in the same clause); `COLUMNS()` for applying an expression across multiple columns; `COALESCE` not `IFNULL`.
- **UDFs / macros** — `duckdb_register(con, "name", dt)` exposes a data.table as a virtual table; SQL macros (`CREATE MACRO`) encapsulate reusable calculations without leaving SQL.
- **Integer division** — DuckDB integer columns stay integer; use `::DOUBLE` cast: `sum(SO)::DOUBLE / sum(AB)`.
- **Always `shutdown = TRUE`** in `DBI::dbDisconnect()` — omitting it leaves the DuckDB process alive.
- **One writer, many readers** — only one writable connection at a time; open analysis sessions with `read_only = TRUE`.
- **`duckdb_register()` does not copy** — do not modify the registered data.table while the connection is open.

## Lahman Notes

- **`X2B` / `X3B`** — R renames the `2B`/`3B` columns when loading into DuckDB; always use `X2B`, `X3B` in SQL.
- **`Salaries` only covers through 2016** — use `SalariesAll WHERE is_actual = TRUE` for any multi-era salary analysis.
- **`Teams` covers through 2025** — used for era-adjusted FIP constants; teamID codes follow Lahman convention (e.g., CHN/CHA/KCA, not CHC/CHW/KC).
- **`IPouts`** = outs recorded (IP × 3); `InnOuts` in `Fielding` is the same concept.
- **Skip on load** — `LahmanData`, `battingLabels`, `fieldingLabels`, `pitchingLabels` are metadata, not data.

## Tests

- Most tests use `:memory:` DuckDB — fast, no file paths, CI-safe.
- The full `setup_baseball_db()` smoke test uses `skip_on_ci()` and `skip_if_not_installed("Lahman")`.
- Run with `devtools::test()`. All 72 tests must pass before committing.

## Git Workaround (macOS sandbox)

macOS sandbox blocks git operations in the project directory. **All git ops go through `/tmp/lahmans-git-work/`:**

```bash
rsync -a --exclude='.git' --exclude='*.duckdb' $PROJ/ /tmp/lahmans-git-work/
# git add / commit / push / gh pr from /tmp/lahmans-git-work/
rsync -a /tmp/lahmans-git-work/.git/refs/ $PROJ/.git/refs/
rsync -a /tmp/lahmans-git-work/.git/objects/ $PROJ/.git/objects/
```

`git checkout` in the project dir will fail — files are correct but the local branch pointer may lag.

## Interactive R Sessions (Analysis Development)

When developing analysis scripts or iterating on charts, use an **interactive R session** instead of re-running the full script each time:

1. Start R in async mode: `bash mode="async" command="R --no-save"`
2. Source shared setup (DB connection, libraries) once
3. Send individual code blocks via `write_bash` to iterate on specific charts or queries
4. Use the `view` tool on saved PNG files to inspect chart output visually
5. Only assemble the final `.R` script once the individual pieces are working

This avoids the 60-90 second penalty of re-running a full analysis script on every change and enables tight visual feedback loops.

**DuckDB CLI for ad-hoc queries:** Use `duckdb ~/Documents/Data/baseball/baseball.duckdb` for quick schema checks (`DESCRIBE`, `SUMMARIZE`) rather than writing throwaway R code.

## R CMD Check

- Non-ASCII characters (em-dashes, box-drawing) in R source cause WARNING — use ASCII `--`.
- `VignetteBuilder: knitr` in DESCRIPTION without actual vignettes causes NOTE — omit it.
- `Depends: R (>= 4.1.0)` is required when using the native pipe `|>`.
- `utils::globalVariables()` in `globals.R` silences NOTEs for data.table NSE column names.
