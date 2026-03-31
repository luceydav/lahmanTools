test_that("setup_baseball_db() builds expected tables and views", {
  # Requires internet access and ~30s to complete -- skip in CI
  skip_on_ci()
  skip_if_offline()

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
  skip_if_offline()

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  setup_baseball_db(dbdir = db_path, overwrite = TRUE)

  expect_error(
    setup_baseball_db(dbdir = db_path, overwrite = FALSE),
    "already exists"
  )
})

# ── helpers ───────────────────────────────────────────────────────────────────

# Write a minimal Spotrac CSV and return its path.
make_spotrac_csv <- function(rows) {
  f <- tempfile(fileext = ".csv")
  data.table::fwrite(as.data.table(rows), f)
  f
}

# Write a minimal USA Today CSV and return its path.
make_usatoday_csv <- function(rows) {
  f <- tempfile(fileext = ".csv")
  data.table::fwrite(as.data.table(rows), f)
  f
}

# ── Spotrac team-ID normalisation ─────────────────────────────────────────────

test_that("SalariesAll normalises Spotrac team codes to Lahman teamIDs", {
  skip_on_ci()
  skip_if_offline()

  # Spotrac uses non-Lahman abbreviations for several teams
  spotrac_rows <- list(
    yearID   = c(2017L, 2018L, 2019L, 2019L, 2019L, 2019L, 2019L, 2019L, 2019L, 2019L, 2019L),
    player   = c("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"),
    team     = c("CHC", "CHW", "KC",  "LAD", "NYM", "NYY", "SD",  "SF",  "STL", "TB",  "WSH"),
    position = rep("SP", 11),
    salary   = rep(600000, 11),   # above 2019 minimum ($555K)
    playerID = paste0("testP", seq_len(11))
  )

  sp_file <- make_spotrac_csv(spotrac_rows)
  on.exit(unlink(sp_file), add = TRUE)

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  suppressWarnings(
    setup_baseball_db(dbdir = db_path, spotrac_file = sp_file, overwrite = TRUE)
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  teams_in_view <- db_query(con,
    "SELECT DISTINCT teamID FROM SalariesAll WHERE source = 'spotrac' ORDER BY teamID"
  )$teamID

  # All remapped to Lahman codes
  expect_true("CHN" %in% teams_in_view)   # CHC → CHN
  expect_true("CHA" %in% teams_in_view)   # CHW → CHA
  expect_true("KCA" %in% teams_in_view)   # KC  → KCA
  expect_true("LAN" %in% teams_in_view)   # LAD → LAN
  expect_true("NYN" %in% teams_in_view)   # NYM → NYN
  expect_true("NYA" %in% teams_in_view)   # NYY → NYA
  expect_true("SDN" %in% teams_in_view)   # SD  → SDN
  expect_true("SFN" %in% teams_in_view)   # SF  → SFN
  expect_true("SLN" %in% teams_in_view)   # STL → SLN
  expect_true("TBA" %in% teams_in_view)   # TB  → TBA
  expect_true("WAS" %in% teams_in_view)   # WSH → WAS

  # Original Spotrac codes must NOT appear
  expect_false(any(c("CHC","CHW","KC","LAD","NYM","NYY","SD","SF","STL","TB","WSH")
                   %in% teams_in_view))
})

test_that("SalariesAll Spotrac branch excludes sub-minimum salary players", {
  skip_on_ci()
  skip_if_offline()

  sp_file <- make_spotrac_csv(list(
    yearID   = c(2019L, 2019L, 2019L),
    player   = c("Below", "AtMin", "Above"),
    team     = c("NYY",   "NYY",   "NYY"),
    position = c("RP",    "SP",    "1B"),
    salary   = c(200000,  555000,  1000000),
    playerID = c("subP01", "minP01", "mlbP01")
  ))
  on.exit(unlink(sp_file), add = TRUE)

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  suppressWarnings(
    setup_baseball_db(dbdir = db_path, spotrac_file = sp_file, overwrite = TRUE)
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  ids <- db_query(con,
    "SELECT playerID FROM SalariesAll WHERE source = 'spotrac' ORDER BY playerID"
  )$playerID

  expect_false("subP01" %in% ids)   # $200K — below 2019 minimum
  expect_true ("minP01" %in% ids)   # exactly at minimum
  expect_true ("mlbP01" %in% ids)   # comfortably above minimum
})

# ── USA Today team-name normalisation ─────────────────────────────────────────

test_that("SalariesAll normalises USA Today team names to Lahman teamIDs", {
  skip_on_ci()
  skip_if_offline()

  # Representative sample of USA Today name variants
  usa_rows <- list(
    playerID       = paste0("usaP", seq_len(14)),
    yearID         = rep(2023L, 14),
    team           = c("Cubs", "White Sox", "Dodgers", "Yankees", "Mets",
                       "Royals", "Cardinals", "Rays", "Guardians",
                       "N.Y. Yankees", "L.A. Dodgers", "Chi. Cubs",
                       "Chic. White Sox", "Padres"),
    salary         = rep(1e6, 14),
    average_annual = rep(1e6, 14),
    years          = c("1 (2023-24)", rep(NA_character_, 13))
  )

  usa_file <- make_usatoday_csv(usa_rows)
  on.exit(unlink(usa_file), add = TRUE)

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  suppressWarnings(
    setup_baseball_db(dbdir = db_path, sal_file = usa_file, overwrite = TRUE)
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  teams_in_view <- db_query(con,
    "SELECT DISTINCT teamID FROM SalariesAll WHERE source = 'usatoday' ORDER BY teamID"
  )$teamID

  expect_true("CHN" %in% teams_in_view)   # Cubs / Chi. Cubs → CHN
  expect_true("CHA" %in% teams_in_view)   # White Sox / Chic. White Sox → CHA
  expect_true("LAN" %in% teams_in_view)   # Dodgers / L.A. Dodgers → LAN
  expect_true("NYA" %in% teams_in_view)   # Yankees / N.Y. Yankees → NYA
  expect_true("NYN" %in% teams_in_view)   # Mets → NYN
  expect_true("KCA" %in% teams_in_view)   # Royals → KCA
  expect_true("SLN" %in% teams_in_view)   # Cardinals → SLN
  expect_true("TBA" %in% teams_in_view)   # Rays → TBA
  expect_true("CLE" %in% teams_in_view)   # Guardians → CLE
  expect_true("SDN" %in% teams_in_view)   # Padres → SDN

  # Original name-style strings must NOT appear
  raw_names <- c("Cubs","White Sox","Dodgers","Yankees","Mets","Royals",
                 "Cardinals","Rays","Guardians","N.Y. Yankees","L.A. Dodgers",
                 "Chi. Cubs","Chic. White Sox","Padres")
  expect_false(any(raw_names %in% teams_in_view))
})

# ── Century-crossing contract year: "2 (1999-01)" must expand to 2001 ─────────

test_that("SalariesAll expands century-crossing 2-digit end years correctly", {
  # A contract "2 (1999-01)" has c_start=1999, c_end should be 2001 (not 1901).
  # The naive left(c_start_str, 2) || "01" = "1901" bug would drop this row.
  usa_rows <- list(
    playerID       = "century_p1",
    yearID         = 1999L,
    team           = "Yankees",
    salary         = 1e7,
    average_annual = 1e7,
    years          = "2 (1999-01)"
  )
  usa_file <- make_usatoday_csv(usa_rows)
  on.exit(unlink(usa_file), add = TRUE)

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  suppressWarnings(
    setup_baseball_db(dbdir = db_path, sal_file = usa_file, overwrite = TRUE)
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Contract should produce imputed rows for both 1999 and 2000 (is_actual=FALSE)
  # or be present as actual for the scraped year (1999)
  years_present <- db_query(con,
    "SELECT DISTINCT yearID FROM SalariesAll
     WHERE playerID = 'century_p1' ORDER BY yearID"
  )$yearID

  # The contract spans 1999-2001; at minimum year 2000 must appear (imputed AAV)
  # which only happens if c_end was correctly parsed as 2001, not dropped as 1901
  expect_true(2000L %in% years_present,
    info = "century-crossing contract (1999-01) dropped — c_end parsed as 1901 instead of 2001")
})


test_that("SalariesAll teamIDs join cleanly to Teams for 2017-2023", {
  skip_on_ci()
  skip_if_offline()

  # Build a Spotrac CSV covering the four teams whose codes differ most
  sp_file <- make_spotrac_csv(list(
    yearID   = c(2019L, 2019L, 2019L, 2019L),
    player   = c("P1", "P2", "P3", "P4"),
    team     = c("CHC", "LAD", "NYM", "STL"),
    position = rep("SP", 4),
    salary   = rep(600000, 4),
    playerID = paste0("jnTest", seq_len(4))
  ))
  on.exit(unlink(sp_file), add = TRUE)

  db_path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(db_path), add = TRUE)

  suppressWarnings(
    setup_baseball_db(dbdir = db_path, spotrac_file = sp_file, overwrite = TRUE)
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # An unmatched teamID produces zero rows in the join
  unmatched <- db_query(con, "
    SELECT DISTINCT s.teamID
    FROM   SalariesAll s
    WHERE  s.source = 'spotrac'
      AND  s.teamID NOT IN (SELECT DISTINCT teamID FROM Teams)
  ")

  expect_equal(nrow(unmatched), 0L,
    info = paste("Unmatched Spotrac teamIDs:", paste(unmatched$teamID, collapse = ", "))
  )
})
