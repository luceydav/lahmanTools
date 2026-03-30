# Contributing to lahmanTools

## Dev Setup

### Prerequisites

- R ≥ 4.1.0 with `devtools`, `data.table`, `duckdb`, `DBI`, `testthat`
- [GitHub Copilot CLI](https://docs.github.com/copilot/concepts/agents/about-copilot-cli) (optional but recommended for AI-assisted development)

### Build the database

```r
library(lahmanTools)
setup_baseball_db()
```

To include extended salary data (2017–2025):

```r
scrape_salaries(years = 2017:2025,
                output_dir = "~/Documents/Data/baseball/mlb_salaries")

setup_baseball_db(
  sal_file = "~/Documents/Data/baseball/mlb_salaries/salaries_2017_2025_with_playerID.csv",
  overwrite = TRUE
)
```

> **Scraped data policy:** USA Today and Spotrac salary CSV files must **not** be redistributed. They are gitignored. See [data-raw/README.md](data-raw/README.md).

### DuckDB MCP Server (AI-assisted development)

When using GitHub Copilot CLI or Claude Code, configuring a local DuckDB MCP server lets the AI agent query `baseball.duckdb` directly during development sessions.

Install the server:

```bash
uv tool install duckdb-mcp-server
```

Then use the package helper to generate and write the config -- it resolves `~` to an absolute path (Python-based MCP servers do not expand it) and merges without clobbering other server entries:

```r
# Preview first
write_mcp_config()

# Write to ~/.copilot/mcp-config.json
write_mcp_config(dry_run = FALSE)
```

`setup_baseball_db()` prints this reminder automatically after a successful build.

> `--readonly` is always enforced by `write_mcp_config()`. Without it an AI agent could modify or corrupt the database.

---

## Branching strategy

```
main ──────── v0.1.0 ──────────────────── v0.2.0 ──▶
               ↑                            ↑
dev ───────────┴── feature/x ── feature/y ──┴──▶
```

| Branch | Purpose | Rules |
|--------|---------|-------|
| `main` | Always release-ready | Never commit directly; merge from `dev` via PR only |
| `dev` | Active development | Default working branch; must pass `R CMD CHECK` before merging to `main` |
| `feature/*` | Discrete changes | Branch off `dev`, merge back to `dev` via PR when ready |
| `hotfix/*` | Urgent production fixes | Branch off `main`, merge to **both** `main` and `dev` |

## Workflow for a new feature

```bash
git checkout dev
git pull origin dev
git checkout -b feature/my-feature

# ... make changes, commit ...

git push origin feature/my-feature
# open PR: feature/my-feature → dev
```

## Releasing a new version

1. On `dev`, bump `Version:` in `DESCRIPTION` and add a section to `NEWS.md`
2. Run `R CMD CHECK` — zero errors and warnings required
3. Open a PR: `dev → main` titled `Release vX.Y.Z`
4. After merge, tag the release:
   ```bash
   git checkout main && git pull
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin vX.Y.Z
   ```
5. Create a GitHub Release from the tag and paste the `NEWS.md` section as release notes

## Versioning

Follows [Semantic Versioning](https://semver.org/):

| Increment | When |
|-----------|------|
| **PATCH** `0.1.x` | Bug fixes, documentation, internal refactors |
| **MINOR** `0.x.0` | New exported functions, new views, backward-compatible changes |
| **MAJOR** `x.0.0` | Breaking changes to the public API or database schema |

Use `0.x.0` while the API is still stabilising.

## Code style

- Base R + `data.table` only — no `tidyverse` imports
- Vectorise over loops; use `data.table` idioms (`:=`, `.SD`, `by=`)
- DuckDB SQL for anything touching the database; never load full tables into R
- Document all exported functions with `roxygen2`; run `devtools::document()` before committing

## Schema diagram

After any change to `setup_db.R` or `stats_views.R`:

```r
Rscript analysis/schema_dm.R   # regenerates man/figures/lahmanTools_schema.svg
```

Commit the updated SVG alongside the schema change.
