test_that("connect_baseball_db() errors with a clear message on missing file", {
  expect_error(
    connect_baseball_db(dbdir = tempfile(fileext = ".duckdb")),
    "not found"
  )
})

test_that("create_stats_views() creates all expected views", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # Stub the minimal Lahman tables the views require
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
      HR INTEGER, BB INTEGER, SO INTEGER, ERA DOUBLE, BAOpp DOUBLE, IBB INTEGER,
      WP INTEGER, HBP INTEGER, BK INTEGER, BFP INTEGER, GF INTEGER,
      R INTEGER, SH INTEGER, SF INTEGER, GIDP INTEGER
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
      teamID VARCHAR, lgID VARCHAR, POS VARCHAR, G INTEGER, GS INTEGER, InnOuts INTEGER,
      PO INTEGER, A INTEGER, E INTEGER, DP INTEGER,
      PB INTEGER, WP INTEGER, SB INTEGER, CS INTEGER, ZR DOUBLE
    )")

  create_stats_views(con)

  views <- DBI::dbListTables(con)
  expect_true("BattingStats"  %in% views)
  expect_true("PitchingStats" %in% views)
  expect_true("FieldingStats" %in% views)
})

test_that("BattingStats has expected derived columns", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

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
    CREATE TABLE Pitching (playerID VARCHAR, yearID INTEGER, stint INTEGER,
      teamID VARCHAR, lgID VARCHAR, W INTEGER, L INTEGER, G INTEGER,
      GS INTEGER, CG INTEGER, SHO INTEGER, SV INTEGER, IPouts INTEGER,
      H INTEGER, ER INTEGER, HR INTEGER, BB INTEGER, SO INTEGER,
      ERA DOUBLE, BAOpp DOUBLE, IBB INTEGER, WP INTEGER, HBP INTEGER, BK INTEGER,
      BFP INTEGER, GF INTEGER, R INTEGER, SH INTEGER, SF INTEGER, GIDP INTEGER)")
  DBI::dbExecute(con, "
    CREATE TABLE Teams (yearID INTEGER, lgID VARCHAR, teamID VARCHAR,
      W INTEGER, L INTEGER, G INTEGER, R INTEGER, H INTEGER, HR INTEGER,
      BB INTEGER, SO INTEGER, RA INTEGER, ER INTEGER, ERA DOUBLE,
      CG INTEGER, SHO INTEGER, IPouts INTEGER, HA INTEGER, HRA INTEGER,
      BBA INTEGER, SOA INTEGER, E INTEGER, DP INTEGER, FP DOUBLE,
      name VARCHAR, park VARCHAR)")
  DBI::dbExecute(con, "
    CREATE TABLE Fielding (playerID VARCHAR, yearID INTEGER, stint INTEGER,
      teamID VARCHAR, lgID VARCHAR, POS VARCHAR, G INTEGER, GS INTEGER,
      InnOuts INTEGER, PO INTEGER, A INTEGER, E INTEGER, DP INTEGER,
      PB INTEGER, WP INTEGER, SB INTEGER, CS INTEGER, ZR DOUBLE)")

  DBI::dbExecute(con, "
    INSERT INTO Batting VALUES
      ('playerA', 2023, 1, 'NYA', 'AL', 150, 500, 80, 140,
       30, 5, 20, 75, 10, 3, 60, 100, 5, 4, 3, 2, 10)")

  create_stats_views(con)

  bs <- db_query(con, "SELECT * FROM BattingStats")
  expected_cols <- c("playerID", "yearID", "AVG", "OBP", "SLG", "OPS",
                     "ISO", "BABIP", "BB_pct", "K_pct", "PA")
  for (col in expected_cols) {
    expect_true(col %in% names(bs),
                label = paste("BattingStats missing column:", col))
  }
})
