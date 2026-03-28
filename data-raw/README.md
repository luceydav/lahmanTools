# data-raw — Reference Scripts

This directory contains scripts used to collect and prepare salary data
for use with lahmanTools. **These scripts are provided for reference only.**

## Files

| File | Source | Notes |
|------|--------|-------|
| `salary_scraper.R` | USA Today MLB salary database | See legal notice below |
| `salaries.R` | Spotrac MLB salary rankings | See legal notice below |

## Legal Notice

Both scripts access publicly visible web pages but may be subject to the
source site's Terms of Service. **Before running either script, you must:**

1. Review the relevant Terms of Service:
   - USA Today: <https://www.usatoday.com/terms-of-service/>
   - Spotrac: <https://www.spotrac.com/terms/>
2. Determine whether your intended use is permitted
3. Obtain any necessary permission

**The output data files (CSVs) are excluded from version control** via
`.gitignore` and must not be redistributed. The scripts do not imply any
rights to use or redistribute the underlying data.

## Alternatives

For projects requiring redistributable salary data consider:

- [MLB Stats API](https://statsapi.mlb.com) — official, free for non-commercial use
- [Baseball Reference](https://www.baseball-reference.com) — licensed data available
- [Chadwick Baseball Bureau](https://github.com/chadwickbureau) — open datasets
