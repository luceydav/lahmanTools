#' Generate or write an MCP server config for baseball.duckdb
#'
#' Writes (or previews) the JSON entry needed to expose `baseball.duckdb` as a
#' local [DuckDB MCP server](https://github.com/motherduckdb/mcp-server-motherduck)
#' for AI tools such as GitHub Copilot CLI and Claude Code.
#'
#' The main pain point this solves: Python-based MCP servers do **not** expand
#' `~` in path arguments, so the database path must be absolute. This function
#' resolves `dbdir` to a full path before writing.
#'
#' When `config_path` already exists, only the `"baseball"` key is updated;
#' all other server entries are preserved. The server runs read-only by default
#' -- omitting `--read-write` prevents an AI agent from modifying or dropping
#' tables.
#'
#' @param dbdir Path to `baseball.duckdb`. Defaults to the `LAHMANS_DBDIR`
#'   environment variable, then `~/Documents/Data/baseball/baseball.duckdb`.
#' @param config_path Path to write the MCP config JSON. Defaults to
#'   `~/.copilot/mcp-config.json` (read by GitHub Copilot CLI).
#' @param dry_run If `TRUE` (default), prints the JSON that would be written
#'   without touching any files. Set `FALSE` to write.
#'
#' @return Invisibly returns `config_path` when written, or `NULL` in dry-run
#'   mode. Called for its side effects.
#' @export
#'
#' @seealso [setup_baseball_db()], [connect_baseball_db()]
#'
#' @examples
#' \dontrun{
#' # Preview first -- nothing is written
#' write_mcp_config()
#'
#' # Write when satisfied with the output
#' write_mcp_config(dry_run = FALSE)
#'
#' # Custom paths (e.g. if DB lives elsewhere)
#' write_mcp_config(dbdir = "/data/baseball/baseball.duckdb", dry_run = FALSE)
#' }
write_mcp_config <- function(dbdir       = NULL,
                              config_path = path.expand("~/.copilot/mcp-config.json"),
                              dry_run     = TRUE) {

  # -- resolve DB path ---------------------------------------------------------
  if (is.null(dbdir)) {
    dbdir <- Sys.getenv(
      "LAHMANS_DBDIR",
      unset = path.expand("~/Documents/Data/baseball/baseball.duckdb")
    )
  }
  dbdir <- path.expand(dbdir)   # ~ is not expanded by Python subprocesses

  # -- build the server entry --------------------------------------------------
  # Uses uvx so no global install is needed; mcp-server-motherduck is fetched
  # automatically. Read-only by default (omit --read-write).
  new_entry <- list(
    command = "uvx",
    args    = list("mcp-server-motherduck", "--db-path", dbdir)
  )

  # -- dry run: print and exit -------------------------------------------------
  if (dry_run) {
    if (requireNamespace("jsonlite", quietly = TRUE)) {
      snippet <- jsonlite::toJSON(
        list(mcpServers = list(baseball = new_entry)),
        auto_unbox = TRUE, pretty = TRUE
      )
    } else {
      # Fallback when jsonlite not installed -- hand-format the snippet
      snippet <- paste0(
        '{\n  "mcpServers": {\n    "baseball": {\n',
        '      "command": "uvx",\n',
        '      "args": ["mcp-server-motherduck", "--db-path", "', dbdir, '"]\n',
        '    }\n  }\n}'
      )
    }
    message("Dry run -- nothing written. Add to ", config_path, ":\n\n", snippet)
    return(invisible(NULL))
  }

  # -- jsonlite required to write ----------------------------------------------
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop(
      "Package 'jsonlite' is required to write the config file.\n",
      "Install it with: install.packages(\"jsonlite\")",
      call. = FALSE
    )
  }

  # -- merge with existing config: preserve other server entries ---------------
  config_path <- path.expand(config_path)
  cfg <- if (file.exists(config_path)) {
    jsonlite::read_json(config_path)
  } else {
    dir.create(dirname(config_path), showWarnings = FALSE, recursive = TRUE)
    list()
  }

  if (is.null(cfg$mcpServers)) cfg$mcpServers <- list()
  cfg$mcpServers[["baseball"]] <- new_entry

  writeLines(
    jsonlite::toJSON(cfg, auto_unbox = TRUE, pretty = TRUE),
    config_path
  )
  message("MCP config written to ", config_path)
  invisible(config_path)
}
