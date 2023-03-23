# list all schema for which tables can be returned
all_schema <- c(
  "aquatic_data",
  "public",
  "spatial",
  "spatial_isc",
  "stream_network"
)

test_that("list_tables returns a query when collect is FALSE", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # check that queries are returned for all schema
  for (i in seq_along(all_schema)) {
    table_list <- list_tables(all_schema[i])
    expect_true(inherits(table_list, "tbl_PqConnection"))
  }

})

test_that("list_tables return matches a table when collect is TRUE", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # check that tables are returned for all schema when collect is TRUE
  for (i in seq_along(all_schema)) {
    table_list <- list_tables(all_schema[i], collect = TRUE)
    expect_equal(unique(table_list$table_schema), all_schema[i])
    expect_gt(nrow(table_list), 0L)
  }

})

test_that("list_tables errors when requesting unavailable schema", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # check that list_table errors if requesting an unknown schema
  expect_error(list_tables("banana"), "schema must be one of ")

})
