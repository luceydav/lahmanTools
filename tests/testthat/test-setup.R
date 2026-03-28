test_that("setup_baseball_db() builds expected tables and views", {
  # Requires the Lahman package and ~30s to complete — skip in CI
  skip_on_ci()
  skip_if_not_installed("Lahman")

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  setup_baseball_db(dbdir = db_path, overwrite = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  all_objects <- DBI::dbListTables(con)

  # Core Lahman tables
  expect_true("Batting"   %in% all_objects)
  expect_true("Pitching"  %in% all_objects)
  expect_true("Teams"     %in% all_objects)
  expect_true("People"    %in% all_objects)
  expect_true("Fielding"  %in% all_objects)
  expect_true("Salaries"  %in% all_objects)
  expect_true("Managers"  %in% all_objects)

  # Stats views
  expect_true("BattingStats"  %in% all_objects)
  expect_true("PitchingStats" %in% all_objects)
  expect_true("FieldingStats" %in% all_objects)

  # Batting has rows
  n <- db_query(con, "SELECT COUNT(*) AS n FROM Batting")$n
  expect_gt(n, 100000L)
})

test_that("setup_baseball_db() errors if file exists and overwrite = FALSE", {
  skip_on_ci()
  skip_if_not_installed("Lahman")

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  setup_baseball_db(dbdir = db_path, overwrite = TRUE)

  expect_error(
    setup_baseball_db(dbdir = db_path, overwrite = FALSE),
    "already exists"
  )
})
