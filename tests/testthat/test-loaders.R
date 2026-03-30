# ── Test helpers ──────────────────────────────────────────────────────────────
# Minimal in-memory DuckDB with all tables required by the loader view helpers.
stub_war_tables <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE People (
      playerID VARCHAR, bbrefID VARCHAR, retroID VARCHAR,
      nameFirst VARCHAR, nameLast VARCHAR,
      birthYear INTEGER, debut VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Pitching (
      playerID VARCHAR, yearID INTEGER, G INTEGER
    )")
  DBI::dbExecute(con, "
    CREATE TABLE SalariesAll (
      playerID VARCHAR, yearID INTEGER, teamID VARCHAR,
      salary DOUBLE, source VARCHAR, is_actual BOOLEAN
    )")
  DBI::dbExecute(con, "
    CREATE TABLE ChadwickIDs (
      key_bbref VARCHAR, key_fangraphs VARCHAR,
      key_mlbam VARCHAR, key_npb VARCHAR,
      name_last VARCHAR, name_first VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE FangraphsBattingWAR (
      playerid VARCHAR, Season INTEGER, WAR DOUBLE
    )")
  DBI::dbExecute(con, "
    CREATE TABLE FangraphsPitchingWAR (
      playerid VARCHAR, Season INTEGER, WAR DOUBLE
    )")
  DBI::dbExecute(con, "
    CREATE TABLE StatcastPitches (
      batter INTEGER, game_year INTEGER,
      launch_speed DOUBLE, launch_angle DOUBLE,
      estimated_ba_using_speedangle DOUBLE,
      estimated_woba_using_speedangle DOUBLE,
      events VARCHAR
    )")
  # era_label macro is required by SalaryPerWAR
  DBI::dbExecute(con, "
    CREATE OR REPLACE MACRO era_label(yr) AS
      CASE
        WHEN yr BETWEEN 1998 AND 2002 THEN 'Pre-Moneyball'
        WHEN yr BETWEEN 2003 AND 2011 THEN 'Moneyball'
        WHEN yr >= 2012              THEN 'Big Data'
        ELSE NULL
      END
  ")
}

# Insert one batter-season: person + chadwick crosswalk + FG batting WAR row.
insert_batter_season <- function(con, player_id, fg_id, year, bat_war,
                                  salary = 1e6, team = "NYN") {
  DBI::dbExecute(con, sprintf(
    "INSERT INTO People VALUES ('%s','%s','%s','Joe','Test',%d,'%d-04-01')",
    player_id, player_id, player_id, year - 25L, year))
  DBI::dbExecute(con, sprintf(
    "INSERT INTO ChadwickIDs VALUES ('%s','%s','999',NULL,NULL,NULL)",
    player_id, fg_id))
  DBI::dbExecute(con, sprintf(
    "INSERT INTO FangraphsBattingWAR VALUES ('%s',%d,%.1f)",
    fg_id, year, bat_war))
  DBI::dbExecute(con, sprintf(
    "INSERT INTO SalariesAll VALUES ('%s',%d,'%s',%.0f,'lahman',TRUE)",
    player_id, year, team, salary))
}

# ── create_player_ids_view_ ───────────────────────────────────────────────────

test_that("create_player_ids_view_ creates PlayerIDs view with expected columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)

  lahmanTools:::create_player_ids_view_(con)

  expect_true("PlayerIDs" %in% DBI::dbListTables(con))
  cols <- names(DBI::dbGetQuery(con, "SELECT * FROM PlayerIDs LIMIT 0"))
  expect_true(all(c("playerID", "bbrefID", "mlbam_id", "fg_id") %in% cols))
})

test_that("PlayerIDs LEFT JOIN returns all People rows even without Chadwick match", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)

  DBI::dbExecute(con,
    "INSERT INTO People VALUES ('ruthba01','ruthba01','ruthbr01','Babe','Ruth',1895,'1914-07-11')")
  lahmanTools:::create_player_ids_view_(con)

  res <- DBI::dbGetQuery(con, "SELECT * FROM PlayerIDs")
  expect_equal(nrow(res), 1L)
  expect_true(is.na(res$mlbam_id) || res$mlbam_id == "")
})

# ── create_war_views_ ─────────────────────────────────────────────────────────

test_that("create_war_views_ creates PlayerWAR and SalaryPerWAR views", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)

  lahmanTools:::create_war_views_(con)

  tbls <- DBI::dbListTables(con)
  expect_true("PlayerWAR"    %in% tbls)
  expect_true("SalaryPerWAR" %in% tbls)
})

test_that("PlayerWAR has expected columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  lahmanTools:::create_war_views_(con)

  cols <- names(DBI::dbGetQuery(con, "SELECT * FROM PlayerWAR LIMIT 0"))
  expect_true(all(c("playerID", "yearID", "bat_war", "pit_war", "total_war") %in% cols))
})

test_that("SalaryPerWAR has war_reliable column", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  lahmanTools:::create_war_views_(con)

  cols <- names(DBI::dbGetQuery(con, "SELECT * FROM SalaryPerWAR LIMIT 0"))
  expect_true("war_reliable" %in% cols)
})

test_that("war_reliable is TRUE for position player in any era", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  insert_batter_season(con, "jonesal01", "1001", 1999L, bat_war = 4.5)
  lahmanTools:::create_war_views_(con)

  res <- DBI::dbGetQuery(con, "SELECT war_reliable FROM SalaryPerWAR")
  expect_equal(nrow(res), 1L)
  expect_true(res$war_reliable[1L])
})

test_that("war_reliable is FALSE for pitcher season before 1985", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  insert_batter_season(con, "smithpi01", "2002", 1984L, bat_war = 0.1)
  # Mark as pitcher (has pitching appearances in 1984)
  DBI::dbExecute(con, "INSERT INTO Pitching VALUES ('smithpi01', 1984, 30)")
  lahmanTools:::create_war_views_(con)

  res <- DBI::dbGetQuery(con, "SELECT war_reliable FROM SalaryPerWAR")
  expect_equal(nrow(res), 1L)
  expect_false(res$war_reliable[1L])
})

test_that("war_reliable is TRUE for pitcher season 1985 or later", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  insert_batter_season(con, "smithpi01", "2002", 2005L, bat_war = 0.1)
  DBI::dbExecute(con, "INSERT INTO Pitching VALUES ('smithpi01', 2005, 30)")
  lahmanTools:::create_war_views_(con)

  res <- DBI::dbGetQuery(con, "SELECT war_reliable FROM SalaryPerWAR")
  expect_equal(nrow(res), 1L)
  expect_true(res$war_reliable[1L])
})

test_that("PlayerWAR sums batting and pitching WAR for two-way players", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)

  DBI::dbExecute(con,
    "INSERT INTO People VALUES ('ohtansh01','ohtansh01','ohtanr01','Shohei','Ohtani',1994,'2018-03-29')")
  DBI::dbExecute(con,
    "INSERT INTO ChadwickIDs VALUES ('ohtansh01','19755',NULL,NULL,NULL,NULL)")
  DBI::dbExecute(con, "INSERT INTO FangraphsBattingWAR  VALUES ('19755', 2023, 5.2)")
  DBI::dbExecute(con, "INSERT INTO FangraphsPitchingWAR VALUES ('19755', 2023, 4.0)")
  lahmanTools:::create_war_views_(con)

  res <- DBI::dbGetQuery(con, "SELECT * FROM PlayerWAR")
  expect_equal(nrow(res), 1L)
  expect_equal(res$bat_war,   5.2)
  expect_equal(res$pit_war,   4.0)
  expect_equal(res$total_war, 9.2)
})

# ── create_statcast_season_view_ ──────────────────────────────────────────────

test_that("create_statcast_season_view_ creates StatcastSeason with expected columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)
  lahmanTools:::create_statcast_season_view_(con)

  expect_true("StatcastSeason" %in% DBI::dbListTables(con))
  cols <- names(DBI::dbGetQuery(con, "SELECT * FROM StatcastSeason LIMIT 0"))
  expect_true(all(c("mlbam_id", "yearID", "avg_exit_velo",
                    "hard_hit_pct", "xBA", "xwOBA") %in% cols))
})

test_that("StatcastSeason aggregates correctly for a single batter-season", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_war_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO StatcastPitches VALUES
      (660271, 2023, 105.0, 28.0, 0.340, 0.410, 'home_run'),
      (660271, 2023,  88.0, 15.0, 0.210, 0.290, 'field_out'),
      (660271, 2023,  98.0, 22.0, 0.290, 0.360, 'single'),
      (660271, 2023, NULL,  NULL, NULL,  NULL,  NULL)
  ")
  lahmanTools:::create_statcast_season_view_(con)

  res <- DBI::dbGetQuery(con, "SELECT * FROM StatcastSeason")
  expect_equal(nrow(res), 1L)
  expect_equal(res$yearID,       2023L)
  expect_equal(res$pitches_seen, 4L)
  expect_equal(res$pa,           3L)   # NULL events row excluded
  # hard_hit_pct: 2 of 3 non-null batted balls >= 95 mph (105, 98) = 2/3
  expect_equal(round(res$hard_hit_pct, 4), round(2/3, 4))
})

# ── Public loader error paths (no network needed) ─────────────────────────────

test_that("load_chadwick_ids errors clearly when baseballr is absent", {
  skip_if(requireNamespace("baseballr", quietly = TRUE),
          "baseballr is installed; cannot test absence path")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_error(load_chadwick_ids(con), "baseballr")
})

test_that("load_fangraphs_war errors when ChadwickIDs table is missing", {
  skip_if_not_installed("baseballr")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_error(load_fangraphs_war(con), "ChadwickIDs")
})

test_that("load_statcast rejects years before 2015", {
  skip_if_not_installed("baseballr")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_error(load_statcast(con, years = 2014L), "2015")
})
