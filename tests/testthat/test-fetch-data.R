test_that("CPUE is calculated correctly", {
  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  cpue <- fetch_cpue(2) %>%
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    ) %>%
    collect()

  # grab raw data for comparison
  vefmap <- fetch_project(2) %>%
    filter(grepl("EF", gear_type)) %>%
    collect()

  # calculate CPUE from raw data
  effort <- vefmap %>%
    distinct(id_survey, id_surveyevent, seconds) %>%
    group_by(id_survey) %>%
    summarise(seconds = sum(seconds))

  vefmap_cpue <- vefmap %>%
    mutate(
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      catch = collected + observed
    ) %>%
    group_by(waterbody, id_site, id_survey, scientific_name) %>%
    summarise(catch = sum(catch, na.rm = TRUE)) %>%
    ungroup() %>%
    complete(nesting(waterbody, id_site, id_survey), scientific_name, fill = list(catch = 0)) %>%
    left_join(effort, by = "id_survey") %>%
    filter(
      !is.na(seconds),
      seconds > 0,
      !is.na(scientific_name)
    ) %>%
    rename(effort_s = seconds) %>%
    mutate(
      effort_h = effort_s / 3600,
      cpue = catch / effort_h
    ) %>%
    select(
      waterbody, id_site, id_survey, scientific_name, effort_s, effort_h, cpue
    )

  # order cpue and raw-calculated cpue so we can compare
  cpue <- cpue %>%
    ungroup() %>%
    select(all_of(colnames(vefmap_cpue))) %>%
    arrange(waterbody, id_site, id_survey, scientific_name)
  vefmap_cpue <- vefmap_cpue %>%
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    ) %>%
    arrange(waterbody, id_site, id_survey, scientific_name)

  expect_equal(cpue, vefmap_cpue)

})

test_that("fetch_table works with basic filters", {
  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  sites <- fetch_table("site") %>%
    filter(waterbody == "Campaspe River") %>%
    collect()

  # test that this has returned something with some values
  expect_gt(nrow(sites), 0L)

  # and should only include Camaspe River in the waterbody field
  expect_equal(unique(sites$waterbody), "Campaspe River")

})

test_that("fetch_query works with basic filters", {
  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # fetch a basic SQL query
  ovens_info <- fetch_query(
    "SELECT waterbody, id_project
     FROM aquatic_data.site a LEFT JOIN aquatic_data.survey b
     ON a.id_site = b.id_site
     WHERE lower(waterbody) LIKE 'ovens%'
     GROUP BY waterbody, id_project
     ORDER by waterbody, id_project"
  )

  # test that a second filter is still applied correctly
  empty_info <- ovens_info %>% filter(waterbody == "Murray River") %>% collect()

  # collect the data sets
  ovens_info <- ovens_info %>% collect()

  # test that this has returned something with some values
  expect_gt(nrow(ovens_info), 0L)
  expect_equal(nrow(empty_info), 0L)

  # and should only include Camaspe River in the waterbody field
  expect_true(all(grepl("Ovens River", ovens_info$waterbody)))

})
