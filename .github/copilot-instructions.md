# Copilot Instructions

## ⛔ STOP — READ THIS FIRST (violations recur every session)

### Response style
- **Caveman-short**: fragments OK. No flattery. No back-and-forth. No full sentences. Just the gist.
- Code task → diff/code only. Inline `# comment` for non-obvious only. No intro. No summary.
- Research/plan → bullets. No paragraphs.
- Ambiguity → `ask_user`. Don't assume.

### Project-specific tool rules — NEVER violate
| What | NEVER | ALWAYS |
|------|-------|--------|
| SQL on baseball.duckdb | `duckdb` CLI in bash | `baseball-query` MCP |
| R package tests | `devtools::test()` in bash | `btw_tool_pkg_test` |
| View PNG output | `view` tool on PNG | report file path only |

> **Note**: Rust CLI tool preferences (`rg`/`bat`/`sd`/`fd`/`eza`/`rip`) are in the system `<custom_instruction>` — not repeated here.

## Tool Preferences

- **SQL**: use `baseball-query` MCP for ALL queries against `baseball.duckdb`. Never use `duckdb` CLI in bash.
- **GitHub**: prefer `gh` CLI in bash for routine ops (`pr list/view/create`, `issue view`, `push/pull`, `release`). Use `github-mcp-server` tools only for complex needs: cross-repo search, CI log analysis, artifact downloads.

## Response Style

Exploration: parallelize tool calls — fan out, don't serialize.

## Visualization

ggplot charts saved via `ggsave()`:
- **Backend**: `ragg` is installed — always use `device = ragg::agg_png` for crisp text antialiasing.
- **Resolution**: `dpi = 300` (not 150). Current scripts use 150 — low for production output.
- **Label sizing**: `LBL_SIZE = 2.9` in `chart_theme.R` is **mm units** (~8 pt). Multiply pt by 0.352778 to convert. Raise `theme_story(base = 13)` if output looks small after dpi change.
- **ggrepel tuning**: `min.segment.length = 0`, `seed = 42` (reproducibility), `max.overlaps = Inf`, `box.padding = 0.5`, `point.padding = 0.3`, `force = 2`. Increase `force`/`box.padding` when labels still overlap after `max.overlaps = Inf`.
- **Quadrant charts**: always use `quad_setup()` from `chart_theme.R`. It computes explicit axis limits and corner positions. Pass `qs$xlim`/`qs$ylim` to `scale_x_*(limits=, expand=expansion(0))` — never omit `expand=expansion(0)` or labels drift inside the data cloud. Use `QUAD_LBL_SIZE` (3.5) for corner text size.
- **Slide/chart subtitle rule — narrative only**: Subtitles tell the *why* (story, conclusion, significance). They must **never** restate what axis labels, quadrant corner text, legends, or other chart annotations already show. Before finalizing a subtitle, read every sentence and ask: "Does the chart already show this visually?" If yes, delete it. Keep only sentences that add information the chart's visual elements cannot convey.
- **LinkedIn slide pattern**: Always use the Act 6 approach — `(p + li_theme() + coord_cartesian(clip = "off")) + labs(title = "Act N: ...", subtitle = "narrative")`. Chart scripts save both PNG and RDS (`saveRDS(p, sub(".png$", ".rds", out_file))`). `playoff_efficiency.R` loads via `readRDS(chart_path("name.rds"))`. Never use `li_wrap_png` for chart slides — it creates two text layers (slide title + chart title) that readers must parse twice.

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
- **`Teams` covers through 2021** — used for era-adjusted FIP constants; teamID codes follow Lahman convention (e.g., CHN/CHA/KCA, not CHC/CHW/KC). Use `ROW_NUMBER() OVER (PARTITION BY teamID ORDER BY yearID DESC)` to get latest lgID for years > 2021.
- **`IPouts`** = outs recorded (IP × 3); `InnOuts` in `Fielding` is the same concept.
- **Skip on load** — `LahmanData`, `battingLabels`, `fieldingLabels`, `pitchingLabels` are metadata, not data.

## Tests

- Most tests use `:memory:` DuckDB — fast, no file paths, CI-safe.
- The full `setup_baseball_db()` smoke test uses `skip_on_ci()` and `skip_if_not_installed("Lahman")`.
- Run with `devtools::test()`. The suite has ~71 `test_that()` blocks (202 assertions) across 6 files. All must pass before committing.

## Git Workaround (macOS sandbox)

macOS sandbox blocks git writes in the project directory. **All git ops go through `/tmp/lahmans-git-work/`:**

```bash
PROJ=/Users/davidlucey/Documents/Projects/lahmans
rsync -a --exclude='.git' --exclude='*.duckdb' $PROJ/ /tmp/lahmans-git-work/
rm -rf /tmp/lahmans-git-work/.git/refs/refs 2>/dev/null || true  # remove stale dup refs
# git add / commit / push from /tmp/lahmans-git-work/
rsync -a /tmp/lahmans-git-work/.git/refs/ $PROJ/.git/refs/
rsync -a /tmp/lahmans-git-work/.git/objects/ $PROJ/.git/objects/
# REQUIRED after every commit: reset project index to HEAD or staged changes accumulate
cd $PROJ && git reset
```

**Critical:** The rsync only syncs `.git/refs/` and `.git/objects/` — it does NOT sync `.git/index`. Without `git reset` after each commit, the project's staging area drifts from HEAD and builds up a backlog of phantom staged changes across sessions. Always run `git reset` in the project dir as the final step.

**Branch strategy:**
- All work commits go to `dev` — **never commit directly to `main`**
- At release: PR `dev → main`, merge, tag
- After merge: immediately `git rebase origin/main` on `dev` so the next cycle starts clean (avoids divergence)
- CRITICAL: The divergence conflict in v0.2.0 happened because earlier sessions broke this rule and committed directly to `main`

`git checkout` in the project dir will fail — files are correct but the local branch pointer may lag.

## Interactive R Sessions (Analysis Development)

When developing analysis scripts or iterating on charts, use an **interactive R session** instead of re-running the full script each time:

1. Start R in async mode: `bash mode="async" command="R --no-save" shellId="r-session"`
2. Manually connect to DuckDB and load libraries (do NOT source the full script — see gotcha below)
3. Source only the SQL/data-processing block once (lines after `dbConnect`, before chart code)
4. Send only the chart code block via `write_bash` to iterate on specific charts or queries
5. Report the saved PNG file path to the user; do NOT call the `view` tool on PNG files.
6. Only assemble the final `.R` script once the individual pieces are working

**`on.exit` gotcha in sourced scripts:** Analysis scripts use `on.exit(dbDisconnect(con, shutdown = TRUE))` at the top level. When you `source()` such a file interactively, R fires the `on.exit` handler when `source()` returns, closing the connection immediately. **Workaround:** connect manually in the session first, then source only the data-processing and chart sections (not the preamble).

```r
# Step 1 — run once to set up the session
suppressPackageStartupMessages({
  library(data.table); library(ggplot2); library(DBI); library(duckdb)
})
db_path <- file.path(path.expand(Sys.getenv("LAHMANS_DBDIR", "~/Documents/Data/baseball")), "baseball.duckdb")
con <- dbConnect(duckdb(), db_path, read_only = TRUE)

# Step 2 — source just the SQL + data wrangling (skip preamble lines)
source("/tmp/roi_data.R")   # or whichever temp file has only the data block

# Step 3 — iterate: edit chart file, source, view
source("/tmp/roi_chart.R")
```

This avoids the 60-90 second penalty of re-running a full analysis script on every change and enables tight visual feedback loops.

**DuckDB CLI for ad-hoc queries:** Use `duckdb $LAHMANS_DBDIR/baseball.duckdb` for quick schema checks (`DESCRIBE`, `SUMMARIZE`) rather than writing throwaway R code.

## MCP Servers

Two MCP servers are configured in `.copilot/mcp-config.json`:

| Server | Command | Purpose |
|--------|---------|---------|
| `baseball` | `duckdb-mcp-server --readonly` | Read-only SQL access to `baseball.duckdb`; path resolved via `$LAHMANS_DBDIR` using Python (no shell expansion — avoids injection) |
| `r-btw` | `btw::btw_mcp_server()` | R package dev tools: test, document, check, coverage, help |

**Prerequisites:**
- `LAHMANS_DBDIR` env var should be set (defaults to `~/Documents/Data/baseball`).
- `duckdb-mcp-server` binary on `PATH` (installed at `~/.local/bin/duckdb-mcp-server`).
- `btw` R package installed in the system library (not renv).

**Using `r-btw` tools:** prefer them over bash for package tasks — `btw_tool_pkg_test`, `btw_tool_pkg_check`, `btw_tool_pkg_coverage`, `btw_tool_pkg_document` all run in-process and are faster than shell invocations.

**`mcptools` is intentionally NOT configured** as an MCP server. `mcptools::mcp_server(session_tools = TRUE)` would expose `list_r_sessions` / `select_r_session`, giving the AI arbitrary R code execution in any session that has called `mcp_session()`. Use the bash async session approach instead (see Interactive R Sessions above).

## Security

The following files are high-value targets for prompt injection and are protected by CODEOWNERS (owner review required on every PR):

- `.github/copilot-instructions.md` — controls AI agent behaviour for all sessions
- `.copilot/mcp-config.json` — controls which MCP servers (execution surfaces) are available

**What prompt injection means here:** a malicious PR that modifies `copilot-instructions.md` could redirect the agent to exfiltrate data, weaken commit checks, or perform unintended operations. The CODEOWNERS rule ensures a human must explicitly approve any change to these files before merge.

**MCP surface area (in order of privilege):**
1. `baseball` (DuckDB, read-only) — SQL queries only; no writes; path constructed programmatically to avoid shell injection.
2. `r-btw` — can read all package source files and run tests/checks. Cannot write files or execute arbitrary shell commands.

**Never add to MCP config without security review:**
- Any server that exposes `eval`, `system()`, `shell()`, or arbitrary R/Python execution.
- `mcptools::mcp_server(session_tools = TRUE)` — see above.
- Any server that takes user-supplied input as a shell argument.

## R CMD Check

- Non-ASCII characters (em-dashes, box-drawing) in R source cause WARNING — use ASCII `--`.
- `VignetteBuilder: knitr` in DESCRIPTION without actual vignettes causes NOTE — omit it.
- `Depends: R (>= 4.1.0)` is required when using the native pipe `|>`.
- `utils::globalVariables()` in `globals.R` silences NOTEs for data.table NSE column names.
