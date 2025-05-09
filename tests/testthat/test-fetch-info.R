test_that("fetch_info functions return matches to a query", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # set up a query that includes the full VEFMAP data set
  vefmap <- fetch_project(2)

  # can manipulate and filter this query with dplyr methods
  vefmap <- vefmap |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    )

  # fetch information on the sites in a data set
  vefmap_site_info <- fetch_site_info(vefmap)
  expect_true(inherits(vefmap_site_info, "tbl_PqConnection"))

  # fetch information on the surveys in a data set
  vefmap_survey_info <- fetch_survey_info(vefmap)
  expect_true(inherits(vefmap_survey_info, "tbl_PqConnection"))

  # fetch information on the species in a data set
  vefmap_species_info <- fetch_species_info(vefmap)
  expect_true(inherits(vefmap_species_info, "tbl_PqConnection"))

})

test_that("fetch_info functions return matches to a table", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # set up a query that includes the full VEFMAP data set
  vefmap <- fetch_project(2)

  # can manipulate and filter this query with dplyr methods
  vefmap <- vefmap |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    )

  # collect this query
  vefmap <- vefmap |> collect()

  # fetch information on the sites in a data set
  vefmap_site_info <- fetch_site_info(vefmap) |> collect()
  expect_equal(unique(vefmap_site_info$waterbody), "Campaspe River")
  expect_gt(nrow(vefmap_site_info), 0L)

  # fetch information on the surveys in a data set
  vefmap_survey_info <- fetch_survey_info(vefmap) |> collect()
  expect_equal(unique(vefmap_survey_info$waterbody), "Campaspe River")
  expect_gt(nrow(vefmap_survey_info), 0L)

  # fetch information on the species in a data set
  vefmap_species_info <- fetch_species_info(vefmap) |> collect()
  expect_equal(unique(vefmap_species_info$scientific_name), "Maccullochella peelii")
  expect_gt(nrow(vefmap_species_info), 0L)

})

test_that("fetch_info functions return matches to a regex pattern", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # fetch information on the sites in a data set
  vefmap_site_info1 <- fetch_site_info(pattern = "^Campa")
  vefmap_site_info2 <- fetch_site_info(
    pattern = "campaspe",
    ignore.case = TRUE
  )
  expect_true(inherits(vefmap_site_info1, "tbl_PqConnection"))
  expect_true(inherits(vefmap_site_info2, "tbl_PqConnection"))
  vefmap_site_info1 <- vefmap_site_info1 |> collect()
  vefmap_site_info2 <- vefmap_site_info2 |> collect()
  expect_equal(vefmap_site_info1, vefmap_site_info2)

  # fetch information on the surveys in a data set
  vefmap_survey_info1 <- fetch_survey_info(pattern = "^Campa")
  vefmap_survey_info2 <- fetch_survey_info(
    pattern = "campaspe",
    ignore.case = TRUE
  )
  expect_true(inherits(vefmap_survey_info1, "tbl_PqConnection"))
  expect_true(inherits(vefmap_survey_info2, "tbl_PqConnection"))
  vefmap_survey_info1 <- vefmap_survey_info1 |> collect()
  vefmap_survey_info2 <- vefmap_survey_info2 |> collect()
  expect_equal(vefmap_survey_info1, vefmap_survey_info2)

  # fetch information on the species in a data set
  vefmap_species_info1 <- fetch_species_info(pattern = "^Maccull")
  vefmap_species_info2 <- fetch_species_info(
    pattern = "^maccull",
    ignore.case = TRUE
  )
  expect_true(inherits(vefmap_species_info1, "tbl_PqConnection"))
  expect_true(inherits(vefmap_species_info2, "tbl_PqConnection"))
  vefmap_species_info1 <- vefmap_species_info1 |> collect()
  vefmap_species_info2 <- vefmap_species_info2 |> collect()
  expect_equal(vefmap_species_info1, vefmap_species_info2)

})
