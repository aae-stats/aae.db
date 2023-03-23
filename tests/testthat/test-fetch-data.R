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

test_that("fetch_project matches return from the sql view", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # internal list of project names by id
  project_list <- c(1, 2, 4, 6:9, 11:12, 14:16)

  # fetch a basic SQL query
  for (i in seq_along(project_list)) {

    # grab R and sql versions of the same table
    value <- fetch_project(project_list[i], collect = TRUE)
    target <- fetch_query(
      paste0("SELECT * FROM aquatic_data.get_project_data(",
      project_list[i],
      ", 1900, 2099)"),
      collect = TRUE
    )

    # sort and remove time extracted field
    value <- value %>%
      arrange(
        id_site, waterbody, site_name, site_desc, id_survey, id_project,
        survey_date, gear_type, id_surveyevent, time_start, condition,
        id_netting, id_sample, id_observation, scientific_name,
        common_name, fork_length_cm, length_cm, collected, observed
      ) %>%
      select(-extracted_ts)
    target <- target %>%
      select(-id_taxon) %>%
      arrange(
        id_site, waterbody, site_name, site_desc, id_survey, id_project,
        survey_date, gear_type, id_surveyevent, time_start, condition,
        id_netting, id_sample, id_observation, scientific_name,
        common_name, fork_length_cm, length_cm, collected, observed
      ) %>%
      mutate(
        id_site = as.integer(id_site),
        id_survey = as.integer(id_survey),
        id_surveyevent = as.integer(id_surveyevent),
        id_observation = as.integer(id_observation),
        id_sample = as.integer(id_sample),
        survey_year = as.numeric(survey_year)
      ) %>%
      select(-extracted_ts)

    # compare
    expect_equal(value, target)

  }

  # test that a non-existent project returns an empty table
  value <- fetch_project(30, collect = TRUE)
  expect_equal(nrow(value), 0L)

  # test that a combination of two projects is collected correctly
  project_sub <- project_list[2:5]
  value <- fetch_project(project_sub, collect = TRUE)
  target <- vector("list", length = length(project_sub))
  for (i in seq_along(target)) {
    target[[i]] <- fetch_query(
      paste0("SELECT * FROM aquatic_data.get_project_data(",
             project_sub[i],
             ", 1900, 2099)"),
      collect = TRUE
    )
  }
  target <- do.call(rbind, target)

  # sort and remove time extracted field
  value <- value %>%
    arrange(
      id_site, waterbody, site_name, site_desc, id_survey, id_project,
      survey_date, gear_type, id_surveyevent, time_start, condition,
      id_netting, id_sample, id_observation, scientific_name,
      common_name, fork_length_cm, length_cm, collected, observed
    ) %>%
    select(-extracted_ts)
  target <- target %>%
    select(-id_taxon) %>%
    arrange(
      id_site, waterbody, site_name, site_desc, id_survey, id_project,
      survey_date, gear_type, id_surveyevent, time_start, condition,
      id_netting, id_sample, id_observation, scientific_name,
      common_name, fork_length_cm, length_cm, collected, observed
    ) %>%
    mutate(
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      id_surveyevent = as.integer(id_surveyevent),
      id_observation = as.integer(id_observation),
      id_sample = as.integer(id_sample),
      survey_year = as.numeric(survey_year)
    ) %>%
    select(-extracted_ts)

  # compare
  expect_equal(value, target)

})
