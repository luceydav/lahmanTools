#' Open a connection to the baseball DuckDB database
#'
#' @param dbdir Path to the `baseball.duckdb` file. Defaults to the value of
#'   the `LAHMANS_DBDIR` environment variable if set, otherwise
#'   `~/Documents/Data/baseball/baseball.duckdb`. Override by setting
#'   `Sys.setenv(LAHMANS_DBDIR = "/your/path/baseball.duckdb")` in
#'   `.Renviron`.
#' @param read_only Open in read-only mode. Default `TRUE` for analysis;
#'   use `FALSE` only when rebuilding via [setup_baseball_db()].
#' @param connections_pane If `TRUE`, registers the connection with the RStudio
#'   Connections pane via the `connections` package (must be installed). The
#'   returned object is still a standard `DBIConnection` and works identically
#'   with all DBI and `db_query()` calls; the only difference is that RStudio
#'   will display it in the Connections pane with a browse/disconnect button.
#'   Silently falls back to a plain `DBI::dbConnect()` when `connections` is
#'   not installed.
#'
#' @return A `DBIConnection` object (or a `connections`-wrapped equivalent when
#'   `connections_pane = TRUE`). Close with
#'   `DBI::dbDisconnect(con, shutdown = TRUE)`.
#' @export
#'
#' @examples
#' \dontrun{
#' # Plain connection
#' con <- connect_baseball_db()
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#'
#' # Visible in RStudio Connections pane
#' con <- connect_baseball_db(connections_pane = TRUE)
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
connect_baseball_db <- function(dbdir = NULL, read_only = TRUE,
                                connections_pane = FALSE) {
  if (is.null(dbdir)) {
    dbdir <- Sys.getenv(
      "LAHMANS_DBDIR",
      unset = path.expand("~/Documents/Data/baseball/baseball.duckdb")
    )
  }
  if (!file.exists(dbdir)) {
    stop(dbdir, " not found. Run setup_baseball_db() first.")
  }

  if (connections_pane && requireNamespace("connections", quietly = TRUE)) {
    connections::connection_open(duckdb::duckdb(),
                                 dbdir = dbdir,
                                 read_only = read_only)
  } else {
    if (connections_pane) {
      message("Package 'connections' not installed; ",
              "using plain DBI::dbConnect() instead.")
    }
    DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = read_only)
  }
}
