test_that("dt_factors_to_char converts factors to character in place", {
  dt <- data.table::data.table(
    x = factor(c("a", "b", "c")),
    y = 1:3,
    z = factor(c("p", "q", "r"))
  )
  result <- dt_factors_to_char(dt)

  expect_equal(class(dt$x), "character")
  expect_equal(class(dt$z), "character")
  expect_equal(class(dt$y), "integer")  # non-factor unchanged
  expect_identical(result, dt)          # returns same object (by reference)
  expect_equal(dt$x, c("a", "b", "c"))
})

test_that("dt_factors_to_char is a no-op on a table with no factors", {
  dt <- data.table::data.table(a = 1:3, b = c("x", "y", "z"))
  dt_factors_to_char(dt)
  expect_equal(class(dt$b), "character")
  expect_equal(class(dt$a), "integer")
})

test_that("clean_names normalises to snake_case", {
  expect_equal(clean_names("Average Annual"),  "average_annual")
  expect_equal(clean_names("Total Value"),     "total_value")
  expect_equal(clean_names("Player Name"),     "player_name")
  expect_equal(clean_names("already_snake"),   "already_snake")
  expect_equal(clean_names("trailing_"),       "trailing")   # strip trailing _
})

test_that("clean_names handles vectors", {
  input  <- c("Average Annual", "Total Value", "Years")
  output <- clean_names(input)
  expect_equal(output, c("average_annual", "total_value", "years"))
})

test_that("db_query returns a data.table", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "CREATE TABLE t (a INTEGER, b VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO t VALUES (1, 'x'), (2, 'y')")

  result <- db_query(con, "SELECT * FROM t ORDER BY a")

  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 2L)
  expect_equal(result$a, c(1L, 2L))
  expect_equal(result$b, c("x", "y"))
})

test_that("db_query passes extra arguments to dbGetQuery", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "CREATE TABLE nums (n INTEGER)")
  DBI::dbExecute(con, "INSERT INTO nums SELECT range FROM range(100)")

  result <- db_query(con, "SELECT * FROM nums ORDER BY n")
  expect_equal(nrow(result), 100L)
})


# --- normalise_player_name ---------------------------------------------------

test_that("normalise_player_name strips asterisks", {
  expect_equal(normalise_player_name("Harper, Bryce*"), "harper, bryce")
})

test_that("normalise_player_name strips suffixes", {
  expect_equal(normalise_player_name("Acuna Jr., Ronald"), "acuna, ronald")
  expect_equal(normalise_player_name("Guerrero Sr., Vladimir"), "guerrero, vladimir")
  expect_equal(normalise_player_name("Smith III, John"), "smith, john")
})

test_that("normalise_player_name transliterates accents", {
  expect_equal(normalise_player_name("Acu\u00f1a, Ronald"), "acuna, ronald")
})

test_that("normalise_player_name fixes UTF-8 mojibake", {
  # "Ã³" is the mojibake for "ó" (UTF-8 bytes read as Latin-1)
  mojibake <- "Can\u00c3\u00b3, Robinson"
  expect_equal(normalise_player_name(mojibake), "cano, robinson")
})

test_that("normalise_player_name normalises initials", {
  expect_equal(normalise_player_name("Martinez, J.D."), "martinez, j d")
  expect_equal(normalise_player_name("Martinez, JD"), "martinez, j d")
  expect_equal(normalise_player_name("Realmuto, JT"), "realmuto, j t")
})

test_that("normalise_player_name strips apostrophes", {
  expect_equal(normalise_player_name("d'Arnaud, Travis"), "darnaud, travis")
})

test_that("normalise_player_name handles vectors", {
  input <- c("Harper, Bryce*", "Acuna Jr., Ronald", "Smith, John")
  result <- normalise_player_name(input)
  expect_length(result, 3L)
  expect_equal(result, c("harper, bryce", "acuna, ronald", "smith, john"))
})


# --- match_player_ids --------------------------------------------------------

# Helper to build a minimal People data.table for testing
make_test_people <- function() {
  data.table::data.table(
    playerID  = c("harpebr03", "acunaro01", "martij06",
                   "darntra01", "smithjo99"),
    nameFirst = c("Bryce",     "Ronald",    "J. D.",
                   "Travis",    "John"),
    nameLast  = c("Harper",    "Acu\u00f1a", "Martinez",
                   "d'Arnaud",  "Smith"),
    debut     = c("2012-04-28", "2018-04-25", "2011-08-11",
                   "2013-04-26", "2020-07-24"),
    finalGame = c(NA,           NA,           NA,
                   "2024-09-29", NA)
  )
}

test_that("match_player_ids Pass 1: exact match works", {
  people <- make_test_people()
  sal <- data.table::data.table(
    player = "Smith, John",
    yearID = 2022L
  )
  match_player_ids(sal, people)
  expect_equal(sal$playerID, "smithjo99")
})

test_that("match_player_ids Pass 2: normalised match catches suffixes + accents", {
  people <- make_test_people()
  sal <- data.table::data.table(
    player = c("Acuna Jr., Ronald", "d'Arnaud, Travis"),
    yearID = c(2023L, 2022L)
  )
  match_player_ids(sal, people)
  expect_equal(sal$playerID, c("acunaro01", "darntra01"))
})

test_that("match_player_ids Pass 2: asterisks stripped", {
  people <- make_test_people()
  sal <- data.table::data.table(
    player = "Harper, Bryce*",
    yearID = 2023L
  )
  match_player_ids(sal, people)
  expect_equal(sal$playerID, "harpebr03")
})

test_that("match_player_ids leaves truly unmatched as NA", {
  people <- make_test_people()
  sal <- data.table::data.table(
    player = "Nonexistent, Player",
    yearID = 2023L
  )
  match_player_ids(sal, people)
  expect_true(is.na(sal$playerID))
})

test_that("match_player_ids Pass 3: disambiguates by year", {
  # Two people with the same name but different eras
  people <- data.table::data.table(
    playerID  = c("johnjr01", "johnjr02"),
    nameFirst = c("Junior",   "Junior"),
    nameLast  = c("Johnson",  "Johnson"),
    debut     = c("1990-04-01", "2018-04-01"),
    finalGame = c("2005-09-30", NA)
  )
  sal <- data.table::data.table(
    player = "Johnson, Junior",
    yearID = 2022L
  )
  match_player_ids(sal, people)
  expect_equal(sal$playerID, "johnjr02")
})
