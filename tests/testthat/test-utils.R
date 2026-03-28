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
