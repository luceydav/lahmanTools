### ── Test helpers ─────────────────────────────────────────────────────────────
# Create all stub tables needed by create_stats_views() in an in-memory DB.
# Extracted once to avoid repetition; call at the top of each test that needs it.
stub_all_tables <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE Batting (
      playerID VARCHAR, yearID INTEGER, stint INTEGER,
      teamID VARCHAR, lgID VARCHAR,
      G INTEGER, AB INTEGER, R INTEGER, H INTEGER,
      X2B INTEGER, X3B INTEGER, HR INTEGER, RBI INTEGER,
      SB INTEGER, CS INTEGER, BB INTEGER, SO INTEGER,
      IBB INTEGER, HBP INTEGER, SH INTEGER, SF INTEGER, GIDP INTEGER
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Pitching (
      playerID VARCHAR, yearID INTEGER, stint INTEGER,
      teamID VARCHAR, lgID VARCHAR,
      W INTEGER, L INTEGER, G INTEGER, GS INTEGER, CG INTEGER,
      SHO INTEGER, SV INTEGER, IPouts INTEGER, H INTEGER, ER INTEGER,
      HR INTEGER, BB INTEGER, SO INTEGER, ERA DOUBLE, BAOpp DOUBLE,
      IBB INTEGER, WP INTEGER, HBP INTEGER, BK INTEGER, BFP INTEGER,
      GF INTEGER, R INTEGER, SH INTEGER, SF INTEGER, GIDP INTEGER
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Teams (
      yearID INTEGER, lgID VARCHAR, teamID VARCHAR,
      W INTEGER, L INTEGER, G INTEGER,
      R INTEGER, H INTEGER, HR INTEGER, BB INTEGER, SO INTEGER,
      RA INTEGER, ER INTEGER, ERA DOUBLE, CG INTEGER, SHO INTEGER,
      IPouts INTEGER, HA INTEGER, HRA INTEGER, BBA INTEGER, SOA INTEGER,
      E INTEGER, DP INTEGER, FP DOUBLE,
      name VARCHAR, park VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Fielding (
      playerID VARCHAR, yearID INTEGER, stint INTEGER,
      teamID VARCHAR, lgID VARCHAR, POS VARCHAR, G INTEGER, GS INTEGER,
      InnOuts INTEGER, PO INTEGER, A INTEGER, E INTEGER, DP INTEGER,
      PB INTEGER, WP INTEGER, SB INTEGER, CS INTEGER, ZR DOUBLE
    )")
  DBI::dbExecute(con, "
    CREATE TABLE People (
      playerID VARCHAR, birthYear INTEGER, debut VARCHAR
    )")
  # SalariesAll is normally a view created by setup_baseball_db(); stub as a
  # table here so LeagueMedianSalary and TeamPayroll can reference it.
  DBI::dbExecute(con, "
    CREATE TABLE SalariesAll (
      yearID INTEGER, teamID VARCHAR, lgID VARCHAR,
      playerID VARCHAR, salary DOUBLE, is_actual BOOLEAN
    )")
}

### ── Tests ────────────────────────────────────────────────────────────────────

test_that("connect_baseball_db() errors with a clear message on missing file", {
  expect_error(
    connect_baseball_db(dbdir = tempfile(fileext = ".duckdb")),
    "not found"
  )
})

test_that("create_stats_views() creates all expected views and macros", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)
  create_stats_views(con)

  objects <- DBI::dbListTables(con)
  for (nm in c("BattingStats", "PitchingStats", "FieldingStats",
               "PlayerAcquisitionType", "LeagueMedianSalary", "TeamPayroll")) {
    expect_true(nm %in% objects, label = paste("missing:", nm))
  }
  # era_label is a macro, not a table; verify it executes without error
  result <- DBI::dbGetQuery(con, "SELECT era_label(2000) AS e")
  expect_equal(result$e, "Pre-Moneyball")
})

test_that("era_label() returns correct era strings and NULL for out-of-range", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)
  create_stats_views(con)

  res <- DBI::dbGetQuery(con, "
    SELECT era_label(2000) AS pre, era_label(2007) AS mono,
           era_label(2015) AS big, era_label(1985) AS old
  ")
  expect_equal(res$pre,  "Pre-Moneyball")
  expect_equal(res$mono, "Moneyball")
  expect_equal(res$big,  "Big Data")
  expect_true(is.na(res$old))  # NULL -> NA in R
})

test_that("BattingStats has expected derived columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO Batting VALUES
      ('playerA', 2023, 1, 'NYA', 'AL', 150, 500, 80, 140,
       30, 5, 20, 75, 10, 3, 60, 100, 5, 4, 3, 2, 10)")

  create_stats_views(con)

  bs <- db_query(con, "SELECT * FROM BattingStats")
  for (col in c("playerID", "yearID", "AVG", "OBP", "SLG", "OPS",
                "ISO", "BABIP", "BB_pct", "K_pct", "PA")) {
    expect_true(col %in% names(bs), label = paste("BattingStats missing:", col))
  }
})

test_that("PlayerAcquisitionType classifies homegrown, young_acq, veteran_acq", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # homegrown: debut year == first year with this team
  # young_acq: arrived post-debut, age < 26 on arrival
  # veteran_acq: arrived post-debut, age >= 26 on arrival
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p1', 1990, '2010-04-01')")  # homegrown
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p2', 1993, '2013-04-01')")  # young_acq
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p3', 1980, '2000-04-01')")  # veteran_acq

  DBI::dbExecute(con, "INSERT INTO Batting (playerID, yearID, teamID) VALUES ('p1', 2010, 'NYN')")
  # p2 debuted 2013 elsewhere, joins NYN at age 24 in 2017
  DBI::dbExecute(con, "INSERT INTO Batting (playerID, yearID, teamID) VALUES ('p2', 2013, 'BOS')")
  DBI::dbExecute(con, "INSERT INTO Batting (playerID, yearID, teamID) VALUES ('p2', 2017, 'NYN')")
  # p3 debuted 2000 elsewhere, joins NYN at age 27 in 2007
  DBI::dbExecute(con, "INSERT INTO Batting (playerID, yearID, teamID) VALUES ('p3', 2000, 'BOS')")
  DBI::dbExecute(con, "INSERT INTO Batting (playerID, yearID, teamID) VALUES ('p3', 2007, 'NYN')")

  create_stats_views(con)

  res <- data.table::as.data.table(DBI::dbGetQuery(con,
    "SELECT playerID, acq_type FROM PlayerAcquisitionType WHERE teamID = 'NYN' ORDER BY playerID"))

  expect_equal(res[playerID == "p1", acq_type], "homegrown")
  expect_equal(res[playerID == "p2", acq_type], "young_acq")
  expect_equal(res[playerID == "p3", acq_type], "veteran_acq")
})

test_that("TeamPayroll and LeagueMedianSalary have expected columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES
      (2023, 'NYN', 'NL', 'p1', 15000000, TRUE),
      (2023, 'NYN', 'NL', 'p2',  5000000, TRUE),
      (2023, 'TBA', 'AL', 'p3',  3000000, TRUE)")

  create_stats_views(con)

  tp <- DBI::dbGetQuery(con, "SELECT * FROM TeamPayroll WHERE yearID = 2023 ORDER BY teamID")
  expect_true(all(c("yearID", "teamID", "total_salary", "n_players",
                    "median_salary", "max_salary") %in% names(tp)))
  nyn <- tp[tp$teamID == "NYN", ]
  expect_equal(nyn$total_salary, 20000000)
  expect_equal(nyn$n_players, 2L)

  lms <- DBI::dbGetQuery(con, "SELECT * FROM LeagueMedianSalary WHERE yearID = 2023")
  expect_true(all(c("yearID", "med_sal", "avg_sal", "n_players") %in% names(lms)))
  expect_equal(lms$n_players, 3L)
})
