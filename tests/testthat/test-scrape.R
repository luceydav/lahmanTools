# --- scrape_salaries input validation -----------------------------------------

test_that("scrape_salaries() rejects unknown year slugs", {
  expect_error(
    scrape_salaries(years = 2030),
    "No URL slug defined"
  )
})

test_that("scrape_salaries() rejects mixed known and unknown years", {
  expect_error(
    scrape_salaries(years = c(2024, 2030)),
    "2030"
  )
})

test_that("scrape_salaries() error message includes the bad year", {
  expect_error(
    scrape_salaries(years = c(2016, 2026)),
    "2016.*2026|2026.*2016"
  )
})
