test_that("write_mcp_config() dry_run prints JSON and writes nothing", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  expect_message(
    write_mcp_config(
      dbdir       = "/fake/baseball.duckdb",
      binary      = "/usr/local/bin/duckdb-mcp-server",
      config_path = tmp,
      dry_run     = TRUE
    ),
    "--readonly"
  )
  expect_false(file.exists(tmp))
})

test_that("write_mcp_config() writes valid JSON with correct structure", {
  skip_if_not_installed("jsonlite")

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  write_mcp_config(
    dbdir       = "/fake/baseball.duckdb",
    binary      = "/usr/local/bin/duckdb-mcp-server",
    config_path = tmp,
    dry_run     = FALSE
  )

  expect_true(file.exists(tmp))
  cfg <- jsonlite::read_json(tmp)
  expect_true("baseball" %in% names(cfg$mcpServers))
  expect_equal(cfg$mcpServers$baseball$command, "/usr/local/bin/duckdb-mcp-server")
  expect_true("--readonly" %in% cfg$mcpServers$baseball$args)
  expect_true("--db-path"  %in% cfg$mcpServers$baseball$args)
})

test_that("write_mcp_config() always uses an absolute db path (no ~)", {
  skip_if_not_installed("jsonlite")

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  write_mcp_config(
    dbdir       = path.expand("~/baseball.duckdb"),
    binary      = "/usr/local/bin/duckdb-mcp-server",
    config_path = tmp,
    dry_run     = FALSE
  )

  cfg    <- jsonlite::read_json(tmp)
  args   <- cfg$mcpServers$baseball$args
  db_arg <- args[[which(args == "--db-path") + 1L]]
  expect_false(grepl("^~", db_arg))
  expect_true(grepl("^/", db_arg))
})

test_that("write_mcp_config() merges: preserves other server entries", {
  skip_if_not_installed("jsonlite")

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  existing <- list(
    mcpServers = list(
      `other-server` = list(command = "/usr/bin/other", args = list("--flag"))
    )
  )
  writeLines(jsonlite::toJSON(existing, auto_unbox = TRUE, pretty = TRUE), tmp)

  write_mcp_config(
    dbdir       = "/fake/baseball.duckdb",
    binary      = "/usr/local/bin/duckdb-mcp-server",
    config_path = tmp,
    dry_run     = FALSE
  )

  cfg <- jsonlite::read_json(tmp)
  expect_true("other-server" %in% names(cfg$mcpServers))
  expect_true("baseball"     %in% names(cfg$mcpServers))
})

test_that("write_mcp_config() warns and returns NULL when binary not found", {
  tmp <- tempfile(fileext = ".json")

  expect_warning(
    result <- write_mcp_config(
      dbdir       = "/fake/baseball.duckdb",
      binary      = "",
      config_path = tmp,
      dry_run     = FALSE
    ),
    "duckdb-mcp-server"
  )
  expect_null(result)
  expect_false(file.exists(tmp))
})
