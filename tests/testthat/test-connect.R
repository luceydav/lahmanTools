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
      playerID VARCHAR, birthYear INTEGER, debut VARCHAR,
      nameFirst VARCHAR, nameLast VARCHAR
    )")
  # SalariesAll is normally a view created by setup_baseball_db(); stub as a
  # table here so LeagueMedianSalary and TeamPayroll can reference it.
  DBI::dbExecute(con, "
    CREATE TABLE SalariesAll (
      yearID INTEGER, teamID VARCHAR, lgID VARCHAR,
      playerID VARCHAR, salary DOUBLE, is_actual BOOLEAN
    )")
  DBI::dbExecute(con, "
    CREATE TABLE SeriesPost (
      yearID INTEGER, round VARCHAR,
      teamIDwinner VARCHAR, lgIDwinner VARCHAR,
      teamIDloser VARCHAR, lgIDloser VARCHAR,
      wins INTEGER, losses INTEGER, ties INTEGER
    )")
  DBI::dbExecute(con, "
    CREATE TABLE AllstarFull (
      playerID VARCHAR, yearID INTEGER, gameNum INTEGER,
      gameID VARCHAR, teamID VARCHAR, lgID VARCHAR,
      GP INTEGER, startingPos VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE AwardsPlayers (
      playerID VARCHAR, awardID VARCHAR, yearID INTEGER,
      lgID VARCHAR, tie VARCHAR, notes VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE HallOfFame (
      playerID VARCHAR, yearID INTEGER, votedBy VARCHAR,
      ballots DOUBLE, needed DOUBLE, votes DOUBLE,
      inducted VARCHAR, category VARCHAR, needed_note VARCHAR
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Appearances (
      yearID INTEGER, teamID VARCHAR, lgID VARCHAR, playerID VARCHAR,
      G_all INTEGER, GS INTEGER, G_batting INTEGER, G_defense INTEGER,
      G_p INTEGER, G_c INTEGER, G_1b INTEGER, G_2b INTEGER,
      G_3b INTEGER, G_ss INTEGER, G_lf INTEGER, G_cf INTEGER,
      G_rf INTEGER, G_of INTEGER, G_dh INTEGER, G_ph INTEGER, G_pr INTEGER
    )")
  DBI::dbExecute(con, "
    CREATE TABLE Managers (
      playerID VARCHAR, yearID INTEGER, teamID VARCHAR, lgID VARCHAR,
      inseason INTEGER, G INTEGER, W INTEGER, L INTEGER,
      rank INTEGER, plyrMgr VARCHAR
    )")
  # PlayerWAR is normally created by load_fangraphs_war(); stub for view tests
  DBI::dbExecute(con, "
    CREATE TABLE PlayerWAR (
      playerID VARCHAR, yearID INTEGER,
      bat_war DOUBLE, pit_war DOUBLE
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
               "PlayerAcquisitionType", "LeagueMedianSalary", "TeamPayroll",
               "PlayoffPayroll", "AllStarConcentration", "AwardSalaryPremium",
               "HOFCareerArc", "PositionalPayroll", "ManagerPerformance")) {
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
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p1', 1990, '2010-04-01', 'Alpha', 'One')")  # homegrown
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p2', 1993, '2013-04-01', 'Beta', 'Two')")  # young_acq
  DBI::dbExecute(con, "INSERT INTO People VALUES ('p3', 1980, '2000-04-01', 'Gamma', 'Three')")  # veteran_acq

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


# --- Stat formula verification ------------------------------------------------

test_that("BattingStats computes correct rate stats", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # Known inputs: 500 AB, 140 H, 30 2B, 5 3B, 20 HR, 60 BB, 100 SO,
  #               4 HBP, 3 SH, 2 SF, 5 IBB, 10 GIDP
  DBI::dbExecute(con, "
    INSERT INTO Batting VALUES
      ('testbat', 2023, 1, 'NYA', 'AL', 150, 500, 80, 140,
       30, 5, 20, 75, 10, 3, 60, 100, 5, 4, 3, 2, 10)")

  create_stats_views(con)
  bs <- db_query(con, "SELECT * FROM BattingStats WHERE playerID = 'testbat'")

  # PA = AB + BB + HBP + SF + SH = 500 + 60 + 4 + 2 + 3 = 569
  expect_equal(bs$PA, 569L)

  # AVG = H / AB = 140 / 500 = 0.280
  expect_equal(bs$AVG, 0.280, tolerance = 1e-6)

  # OBP = (H + BB + HBP) / (AB + BB + HBP + SF) = 204 / 566
  expect_equal(bs$OBP, 204 / 566, tolerance = 1e-6)


  # SLG = (H + X2B + 2*X3B + 3*HR) / AB = (140+30+10+60) / 500 = 0.480
  expect_equal(bs$SLG, 0.480, tolerance = 1e-6)

  # OPS = OBP + SLG
  expect_equal(bs$OPS, 204 / 566 + 0.480, tolerance = 1e-6)

  # ISO = (X2B + 2*X3B + 3*HR) / AB = (30+10+60) / 500 = 0.200
  expect_equal(bs$ISO, 0.200, tolerance = 1e-6)

  # BABIP = (H - HR) / (AB - SO - HR + SF) = 120 / 382
  expect_equal(bs$BABIP, 120 / 382, tolerance = 1e-6)

  # BB% = BB / PA = 60 / 569
  expect_equal(bs$BB_pct, 60 / 569, tolerance = 1e-6)

  # K% = SO / PA = 100 / 569
  expect_equal(bs$K_pct, 100 / 569, tolerance = 1e-6)
})

test_that("BattingStats returns NULL for zero-AB player", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # Pitcher with 0 AB, 1 BB (only walked once)
  DBI::dbExecute(con, "
    INSERT INTO Batting VALUES
      ('zeroab', 2023, 1, 'NYA', 'AL', 5, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0)")

  create_stats_views(con)
  bs <- db_query(con, "SELECT * FROM BattingStats WHERE playerID = 'zeroab'")

  expect_true(is.na(bs$AVG))
  expect_true(is.na(bs$SLG))
  expect_equal(bs$PA, 1L)  # 0 + 1 + 0 + 0 + 0
})

test_that("PitchingStats computes correct rate stats with FIP", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # League totals for FIP constant calculation
  DBI::dbExecute(con, "
    INSERT INTO Teams (yearID, lgID, teamID, IPouts, HRA, BBA, SOA, ER)
    VALUES (2023, 'AL', 'T1', 65000, 2250, 7000, 20000, 9000),
           (2023, 'AL', 'T2', 65000, 2250, 7000, 20000, 9000)")

  # Player: 200 IP (600 IPouts), 180 H, 75 ER, 20 HR, 55 BB, 190 SO, 14W-8L, 8 HBP
  DBI::dbExecute(con, "
    INSERT INTO Pitching VALUES
      ('testpit', 2023, 1, 'T1', 'AL',
       14, 8, 32, 32, 2, 1, 0, 600, 180, 75,
       20, 55, 190, 3.38, 0.243,
       3, 5, 8, 1, 820, 0, 90, 2, 3, 8)")

  create_stats_views(con)
  ps <- db_query(con, "SELECT * FROM PitchingStats WHERE playerID = 'testpit'")

  # IP = IPouts / 3 = 200.0
  expect_equal(ps$IP, 200.0, tolerance = 1e-6)

  # WHIP = (BB + H) * 3 / IPouts = (55+180)*3/600 = 1.175
  expect_equal(ps$WHIP, 1.175, tolerance = 1e-6)

  # K/9 = SO * 27 / IPouts = 190*27/600 = 8.55
  expect_equal(ps$K_9, 8.55, tolerance = 1e-6)

  # BB/9 = BB * 27 / IPouts = 55*27/600 = 2.475
  expect_equal(ps$BB_9, 2.475, tolerance = 1e-6)

  # HR/9 = HR * 27 / IPouts = 20*27/600 = 0.9
  expect_equal(ps$HR_9, 0.9, tolerance = 1e-6)

  # K/BB = SO / BB = 190/55
  expect_equal(ps$K_BB, 190 / 55, tolerance = 1e-6)

  # Win% = W / (W+L) = 14/22
  expect_equal(ps$Win_pct, 14 / 22, tolerance = 1e-6)

  # FIP = (13*HR + 3*(BB+HBP) - 2*SO) / IP + FIP_constant
  # lg_IPouts = 130000, lg_HR = 4500, lg_BB = 14000, lg_SO = 40000, lg_ER = 18000
  lg_ERA <- 18000 * 27.0 / 130000
  fip_c  <- lg_ERA - (13.0 * 4500 + 3.0 * 14000 - 2.0 * 40000) / (130000 / 3.0)
  expected_fip <- (13.0 * 20 + 3.0 * (55 + 8) - 2.0 * 190) / 200.0 + fip_c
  expect_equal(ps$FIP, expected_fip, tolerance = 1e-4)
  expect_equal(ps$FIP_constant, fip_c, tolerance = 1e-4)
})

test_that("PitchingStats returns NULL for zero-IPouts pitcher", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO Pitching (playerID, yearID, stint, teamID, lgID,
      W, L, G, GS, CG, SHO, SV, IPouts, H, ER, HR, BB, SO, ERA, BAOpp,
      IBB, WP, HBP, BK, BFP, GF, R, SH, SF, GIDP)
    VALUES ('zeroip', 2023, 1, 'NYA', 'AL',
      0, 0, 1, 0, 0, 0, 0, 0, 2, 3, 1, 1, 0, NULL, NULL,
      0, 0, 0, 0, 4, 0, 3, 0, 0, 0)")

  create_stats_views(con)
  ps <- db_query(con, "SELECT * FROM PitchingStats WHERE playerID = 'zeroip'")

  expect_true(is.na(ps$WHIP))
  expect_true(is.na(ps$K_9))
  expect_true(is.na(ps$FIP))
  expect_equal(ps$IP, 0.0)
})

test_that("FieldingStats computes correct metrics", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # Known: PO=300, A=150, E=10, G=150, InnOuts=3600
  DBI::dbExecute(con, "
    INSERT INTO Fielding VALUES
      ('testfld', 2023, 1, 'NYA', 'AL', 'SS', 150, 148,
       3600, 300, 150, 10, 40,
       NULL, NULL, NULL, NULL, NULL)")

  create_stats_views(con)
  fs <- db_query(con, "SELECT * FROM FieldingStats WHERE playerID = 'testfld'")

  # FPCT = (PO + A) / (PO + A + E) = 450 / 460
  expect_equal(fs$FPCT, 450 / 460, tolerance = 1e-6)

  # RF/9 = (PO + A) * 27 / InnOuts = 450 * 27 / 3600 = 3.375
  expect_equal(fs$RF_9, 3.375, tolerance = 1e-6)

  # RF/G = (PO + A) / G = 450 / 150 = 3.0
  expect_equal(fs$RF_G, 3.0, tolerance = 1e-6)
})

test_that("PlayoffPayroll has expected columns and returns rows for seeded data", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO SeriesPost VALUES (2023,'WS','TEX','AL','ARI','NL',4,1,0)")
  DBI::dbExecute(con, "
    INSERT INTO Teams (yearID, lgID, teamID, name, park, W, L, G,
      R, H, HR, BB, SO, RA, ER, ERA, CG, SHO, IPouts,
      HA, HRA, BBA, SOA, E, DP, FP)
    VALUES (2023,'AL','TEX','Texas Rangers','GlobeLife',90,72,162,
      800,1400,200,500,1300,700,650,3.80,10,12,4374,
      1350,180,480,1250,95,130,0.985)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES (2023,'TEX','AL','player01',5000000,TRUE)")
  create_stats_views(con)

  pp <- DBI::dbGetQuery(con, "SELECT * FROM PlayoffPayroll WHERE yearID = 2023")
  expect_true(nrow(pp) >= 1L)
  expect_true(all(c("yearID","teamID","total_salary","rounds_won","won_ws","era") %in% names(pp)))
  expect_equal(pp$won_ws[pp$teamID == "TEX"], 1L)
})

test_that("AllStarConcentration has expected columns and aggregates correctly", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO AllstarFull VALUES
      ('plyr01', 2010, 0, 'ALS2010', 'NYA', 'AL', 1, 'SS'),
      ('plyr02', 2010, 0, 'ALS2010', 'NYA', 'AL', 1, NULL)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES
      (2010,'NYA','AL','plyr01',10000000,TRUE),
      (2010,'NYA','AL','plyr02', 5000000,TRUE),
      (2010,'NYA','AL','plyr03', 2000000,TRUE)")
  DBI::dbExecute(con, "
    INSERT INTO Teams (yearID, lgID, teamID, name, park, W, L, G,
      R, H, HR, BB, SO, RA, ER, ERA, CG, SHO, IPouts,
      HA, HRA, BBA, SOA, E, DP, FP)
    VALUES (2010,'AL','NYA','New York Yankees','Yankee Stadium',95,67,162,
      850,1500,210,520,1350,720,670,3.90,8,14,4400,
      1400,185,495,1280,90,135,0.987)")
  create_stats_views(con)

  ac <- DBI::dbGetQuery(con, "SELECT * FROM AllStarConcentration WHERE yearID = 2010 AND teamID = 'NYA'")
  expect_equal(nrow(ac), 1L)
  expect_equal(ac$n_allstars, 2L)
  expect_equal(ac$n_allstar_starts, 1L)
  expect_true(all(c("allstar_rate","total_salary","era") %in% names(ac)))
})

test_that("AwardSalaryPremium filters to key awards and computes salary_delta", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "
    INSERT INTO People VALUES ('mvp01', 1978, '2005-04-01', 'Mike', 'Vee')")
  DBI::dbExecute(con, "
    INSERT INTO AwardsPlayers VALUES
      ('mvp01','Most Valuable Player',2010,'AL',NULL,NULL),
      ('mvp01','Unimportant Award',    2011,'AL',NULL,NULL)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES
      (2010,'NYA','AL','mvp01', 8000000,TRUE),
      (2011,'NYA','AL','mvp01',18000000,TRUE)")
  DBI::dbExecute(con, "
    INSERT INTO PlayerWAR VALUES ('mvp01',2010,6.5,0.0),('mvp01',2011,5.2,0.0)")
  create_stats_views(con)

  asp <- DBI::dbGetQuery(con, "SELECT * FROM AwardSalaryPremium WHERE playerID = 'mvp01'")
  expect_equal(nrow(asp), 1L)
  expect_equal(asp$awardID, "Most Valuable Player")
  expect_equal(asp$salary_delta, 10000000)
})

test_that("HOFCareerArc includes only inducted players and has era column", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "INSERT INTO People VALUES ('hof01', 1960, '1985-04-08', 'Hank', 'Hofmann')")
  DBI::dbExecute(con, "
    INSERT INTO HallOfFame VALUES
      ('hof01',2005,'BBWAA',520,390,450,'Y','Player',NULL),
      ('notyet',2010,'BBWAA',510,383,200,'N','Player',NULL)")
  DBI::dbExecute(con, "
    INSERT INTO PlayerWAR VALUES ('hof01',2000,7.2,0.0),('hof01',2001,5.1,0.0)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES
      (2000,'NYA','AL','hof01',6000000,TRUE),
      (2001,'NYA','AL','hof01',7500000,TRUE)")
  create_stats_views(con)

  arc <- DBI::dbGetQuery(con, "SELECT * FROM HOFCareerArc ORDER BY yearID")
  expect_true(nrow(arc) >= 2L)
  expect_true(all(arc$playerID == "hof01"))
  expect_true("years_before_induction" %in% names(arc))
  expect_true("era" %in% names(arc))
})

test_that("PositionalPayroll assigns primary position from Appearances", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  # Player appeared mostly at SS (80 games) vs 2B (20 games)
  DBI::dbExecute(con, "
    INSERT INTO Appearances VALUES
      (2015,'NYA','AL','pos01',
       100, 98, 100, 100,
       0, 0, 0, 20, 0, 80, 0, 0, 0, 80, 0, 0, 0)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES (2015,'NYA','AL','pos01',4000000,TRUE)")
  DBI::dbExecute(con, "
    INSERT INTO PlayerWAR VALUES ('pos01',2015,3.5,0.0)")
  create_stats_views(con)

  pp <- DBI::dbGetQuery(con, "SELECT * FROM PositionalPayroll WHERE playerID = 'pos01'")
  expect_equal(nrow(pp), 1L)
  expect_equal(pp$primary_pos, "SS")
  expect_true("salary_per_war" %in% names(pp))
})

test_that("ManagerPerformance computes win_pct and joins payroll", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  stub_all_tables(con)

  DBI::dbExecute(con, "INSERT INTO People VALUES ('mgr01', 1955, '1975-04-01', 'Bob', 'Manager')")
  DBI::dbExecute(con, "
    INSERT INTO Managers VALUES ('mgr01',2018,'BOS','AL',1,162,108,54,1,NULL)")
  DBI::dbExecute(con, "
    INSERT INTO Teams (yearID, lgID, teamID, name, park, W, L, G,
      R, H, HR, BB, SO, RA, ER, ERA, CG, SHO, IPouts,
      HA, HRA, BBA, SOA, E, DP, FP)
    VALUES (2018,'AL','BOS','Boston Red Sox','Fenway Park',108,54,162,
      876,1490,208,514,1290,647,603,3.75,9,16,4374,
      1290,172,472,1303,86,128,0.988)")
  DBI::dbExecute(con, "
    INSERT INTO SalariesAll VALUES
      (2018,'BOS','AL','plyrA',10000000,TRUE),
      (2018,'BOS','AL','plyrB', 8000000,TRUE)")
  create_stats_views(con)

  mp <- DBI::dbGetQuery(con, "SELECT * FROM ManagerPerformance WHERE playerID = 'mgr01'")
  expect_equal(nrow(mp), 1L)
  expect_equal(mp$win_pct, 108 / 162, tolerance = 1e-6)
  expect_equal(mp$total_salary, 18000000)
  expect_true("era" %in% names(mp))
})
