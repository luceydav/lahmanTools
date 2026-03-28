#' Open a connection to the baseball DuckDB database
#'
#' @param dbdir Path to the `baseball.duckdb` file. Defaults to the value of
#'   the `LAHMANS_DBDIR` environment variable if set, otherwise
#'   `~/Documents/Data/baseball/baseball.duckdb`. Override by setting
#'   `Sys.setenv(LAHMANS_DBDIR = "/your/path/baseball.duckdb")` in
#'   `.Renviron`.
#' @param read_only Open in read-only mode. Default `TRUE` for analysis;
#'   use `FALSE` only when rebuilding via [setup_baseball_db()].
#'
#' @return A `DBIConnection` object. Close with
#'   `DBI::dbDisconnect(con, shutdown = TRUE)`.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect_baseball_db()
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
connect_baseball_db <- function(dbdir = NULL, read_only = TRUE) {
  if (is.null(dbdir)) {
    dbdir <- Sys.getenv(
      "LAHMANS_DBDIR",
      unset = path.expand("~/Documents/Data/baseball/baseball.duckdb")
    )
  }
  if (!file.exists(dbdir)) {
    stop(dbdir, " not found. Run setup_baseball_db() first.")
  }
  DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = read_only)
}
