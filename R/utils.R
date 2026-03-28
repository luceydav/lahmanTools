#' Convert factor columns to character in a data.table
#'
#' Converts all factor columns in a `data.table` to character in-place (by
#' reference). Prevents DuckDB from inferring incompatible per-table ENUM
#' domains for the same column name, which causes cross-table join failures.
#'
#' @param dt A `data.table`. Modified by reference; no return value needed.
#'
#' @return `dt` invisibly.
#' @export
#'
#' @examples
#' library(data.table)
#' dt <- data.table(x = factor(c("a", "b")), y = 1:2)
#' dt_factors_to_char(dt)
#' class(dt$x)  # "character"
dt_factors_to_char <- function(dt) {
  factor_cols <- names(dt)[vapply(dt, is.factor, logical(1L))]
  if (length(factor_cols))
    dt[, (factor_cols) := lapply(.SD, as.character), .SDcols = factor_cols]
  invisible(dt)
}

#' Normalise column names to snake_case
#'
#' Converts strings like `"Average Annual"` or `"Total Value"` to lowercase
#' snake_case (`"average_annual"`, `"total_value"`), stripping trailing
#' underscores.  Used primarily to normalise scraped HTML field names before
#' writing to disk.
#'
#' @param x Character vector of names to normalise.
#'
#' @return Character vector the same length as `x`.
#' @export
#'
#' @examples
#' clean_names(c("Average Annual", "Total Value", "Player Name"))
#' # [1] "average_annual" "total_value"    "player_name"
clean_names <- function(x) {
  gsub("_+$", "", tolower(gsub("[^[:alnum:]]+", "_", x)))
}

#' Query a DuckDB connection and return a data.table
#'
#' Thin wrapper around [DBI::dbGetQuery()] that always returns a
#' `data.table` rather than a `data.frame`. Reduces session boilerplate when
#' running ad-hoc SQL against `baseball.duckdb`.
#'
#' @param con A `DBIConnection` object, typically from
#'   [connect_baseball_db()].
#' @param sql A single SQL string.
#' @param ... Additional arguments passed to [DBI::dbGetQuery()].
#'
#' @return A `data.table`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db()
#' db_query(con, "SELECT yearID, AVG(salary) FROM Salaries GROUP BY yearID")
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
db_query <- function(con, sql, ...) {
  data.table::as.data.table(DBI::dbGetQuery(con, sql, ...))
}
