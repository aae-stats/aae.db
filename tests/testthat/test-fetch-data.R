# load CPUE data from netting for comparison
test_data <- read_csv(
  test_path("resources", "netting-test-data.csv"),
  show_col_types = FALSE
)
test_data_ef <- read_csv(
  test_path("resources", "ef-test-data.csv"),
  show_col_types = FALSE
)

test_that("CPUE is calculated correctly for EF data", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  cpue <- fetch_cpue(2) |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii",
      grepl("EF", gear_type)
    ) |>
    collect()

  # grab raw data for comparison
  vefmap <- fetch_project(2) |>
    filter(grepl("EF", gear_type)) |>
    collect()

  # calculate CPUE from raw data
  effort <- vefmap |>
    distinct(id_survey, id_surveyevent, seconds) |>
    group_by(id_survey) |>
    summarise(seconds = sum(seconds))

  vefmap_cpue <- vefmap |>
    mutate(
      collected = as.integer(collected),
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      catch = collected + observed
    ) |>
    group_by(waterbody, id_site, id_survey, scientific_name) |>
    summarise(catch = sum(catch, na.rm = TRUE)) |>
    ungroup() |>
    complete(nesting(waterbody, id_site, id_survey), scientific_name, fill = list(catch = 0)) |>
    left_join(effort, by = "id_survey") |>
    filter(
      !is.na(seconds),
      seconds > 0,
      !is.na(scientific_name)
    ) |>
    rename(effort_s = seconds) |>
    mutate(
      effort_h = effort_s / 3600,
      cpue = catch / effort_h
    ) |>
    select(
      waterbody, id_site, id_survey, scientific_name, effort_s, effort_h, cpue
    )

  # order cpue and raw-calculated cpue so we can compare
  cpue <- cpue |>
    ungroup() |>
    select(all_of(colnames(vefmap_cpue))) |>
    arrange(waterbody, id_site, id_survey, scientific_name)
  vefmap_cpue <- vefmap_cpue |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    ) |>
    arrange(waterbody, id_site, id_survey, scientific_name)

  expect_equal(cpue, vefmap_cpue)

})

test_that("CPUE is calculated correctly for netting data", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  cpue <- fetch_cpue(2) |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    ) |>
    collect()

  # grab raw data for comparison
  vefmap <- fetch_project(2) |>
    collect()

  # calculate CPUE from raw data
  effort <- vefmap |>
    distinct(
      id_survey, id_surveyevent, seconds, soak_minutes_per_unit, gear_count
    ) |>
    group_by(id_survey) |>
    summarise(
      seconds = sum(seconds),
      soak_minutes = sum(soak_minutes_per_unit),
      gear_count = sum(gear_count)
    )

  vefmap_cpue <- vefmap |>
    mutate(
      collected = as.integer(collected),
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      catch = collected + observed
    ) |>
    group_by(waterbody, id_site, id_survey, scientific_name, gear_type) |>
    summarise(catch = sum(catch, na.rm = TRUE)) |>
    ungroup() |>
    complete(
      nesting(waterbody, id_site, id_survey, gear_type),
      scientific_name,
      fill = list(catch = 0)
    ) |>
    left_join(effort, by = "id_survey") |>
    filter(!is.na(scientific_name)) |>
    rename(
      effort_s = seconds,
      effort_soak_minutes = soak_minutes,
      effort_gear_count = gear_count
    ) |>
    mutate(
      effort_h = effort_s / 3600,
      effort_specific = ifelse(
        grepl("EF", gear_type), effort_h, effort_gear_count
      ),
      cpue = catch / effort_specific
    ) |>
    select(
      waterbody, id_site, id_survey,
      scientific_name, gear_type,
      effort_h, effort_gear_count, effort_specific,
      cpue
    )

  # order cpue and raw-calculated cpue so we can compare
  cpue <- cpue |>
    ungroup() |>
    select(all_of(colnames(vefmap_cpue))) |>
    arrange(waterbody, id_site, id_survey, scientific_name)
  vefmap_cpue <- vefmap_cpue |>
    filter(
      waterbody == "Campaspe River",
      scientific_name == "Maccullochella peelii"
    ) |>
    arrange(waterbody, id_site, id_survey, scientific_name)

  expect_equal(cpue, vefmap_cpue)

})

test_that("CPUE from netting data matches AAEDB exports", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  value1 <- fetch_cpue(c(1, 2, 4, 6, 11, 15, 20)) |>
    filter(grepl("FYKE|BT|CRAY|SEINE", gear_type)) |>
    collect()

  # filter to target gears and years
  value <- value1 |>
    filter(
      scientific_name %in% unique(test_data$scientific_name),
      id_survey %in% unique(test_data$id_survey),
      catch > 0
    )
  target <- test_data |>
    filter(
      scientific_name %in% unique(value$scientific_name),
      id_survey %in% unique(value$id_survey)
    )

  # match columns
  value <- value |>
    mutate(
      effort_h = effort_soak_minutes * 60 / 3600,
      cpue = catch / effort_h
    ) |>
    rename(
      net_count = effort_gear_count,
      total_fish = catch
    ) |>
    select(
      waterbody,
      id_site,
      site_name,
      id_survey,
      survey_date,
      survey_year,
      gear_type,
      net_count,
      scientific_name,
      total_fish,
      effort_h,
      cpue
    )
  target <- target |> select(all_of(colnames(value)))

  # reorder
  value <- value |>
    arrange(id_survey, scientific_name)
  target <- target |>
    arrange(id_survey, scientific_name)

  # test if equal
  expect_equal(value, target)

})

test_that("CPUE from EF data matches AAEDB exports", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  value1 <- fetch_cpue(c(2, 4)) |>
    filter(grepl("EF", gear_type)) |>
    collect()

  # filter to target gears and years
  value <- value1 |>
    filter(
      scientific_name %in% unique(test_data_ef$scientific_name),
      id_survey %in% unique(test_data_ef$id_survey),
      catch > 0
    )
  target <- test_data_ef |>
    filter(
      scientific_name %in% unique(value$scientific_name),
      id_survey %in% unique(value$id_survey)
    )

  # match columns
  value <- value |>
    rename(total_fish = catch) |>
    select(
      waterbody,
      id_site,
      site_name,
      id_survey,
      survey_date,
      survey_year,
      gear_type,
      scientific_name,
      total_fish,
      effort_h,
      cpue
    )
  target <- target |> select(all_of(colnames(value)))

  # reorder
  value <- value |>
    arrange(id_survey, scientific_name)
  target <- target |>
    arrange(id_survey, scientific_name)

  # test if equal
  expect_equal(value, target)

})

test_that("CPUE is calculated correctly for multiple projects", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  cpue <- fetch_cpue(c(2, 4, 6, 11, 15)) |>
    collect()

  # grab raw data for comparison
  vefmap <- fetch_project(c(2, 4, 6, 11, 15)) |>
    collect()

  # calculate CPUE from raw data
  effort <- vefmap |>
    distinct(
      id_survey, id_surveyevent, seconds, soak_minutes_per_unit, gear_count
    ) |>
    group_by(id_survey) |>
    summarise(
      seconds = sum(seconds),
      soak_minutes = sum(soak_minutes_per_unit),
      gear_count = sum(gear_count)
    )

  vefmap_cpue <- vefmap |>
    mutate(
      collected = as.integer(collected),
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      catch = collected + observed
    ) |>
    group_by(waterbody, id_site, id_survey, scientific_name, gear_type) |>
    summarise(catch = sum(catch, na.rm = TRUE)) |>
    ungroup() |>
    complete(
      nesting(waterbody, id_site, id_survey, gear_type),
      scientific_name,
      fill = list(catch = 0)
    ) |>
    left_join(effort, by = "id_survey") |>
    filter(!is.na(scientific_name)) |>
    rename(
      effort_s = seconds,
      effort_soak_minutes = soak_minutes,
      effort_gear_count = gear_count
    ) |>
    mutate(
      effort_h = effort_s / 3600,
      effort_specific = ifelse(
        grepl("EF", gear_type), effort_h, effort_gear_count
      ),
      cpue = catch / effort_specific
    ) |>
    select(
      waterbody, id_site, id_survey,
      scientific_name, gear_type,
      effort_h, effort_gear_count, effort_specific,
      catch, cpue
    )

  # order cpue and raw-calculated cpue so we can compare
  cpue <- cpue |>
    ungroup() |>
    select(all_of(colnames(vefmap_cpue))) |>
    arrange(
      waterbody, id_site, id_survey, scientific_name,
      effort_specific, catch, cpue
    )
  vefmap_cpue <- vefmap_cpue |>
    arrange(
      waterbody, id_site, id_survey, scientific_name,
      effort_specific, catch, cpue
    )

  # check these are equal
  expect_equal(cpue, vefmap_cpue, tolerance = 1e-10)

})

test_that("CPUE is calculated correctly for a system", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()

  skip_if(!aaedb_available)

  # grab data with CPUE already calculated
  cpue <- fetch_cpue(reporting_system = "Lower Goulburn System") |>
    collect()

  # grab raw data for comparison
  vefmap <- fetch_project(reporting_system = "Lower Goulburn System") |>
    collect()

  # calculate CPUE from raw data
  effort <- vefmap |>
    distinct(
      id_survey, id_surveyevent, seconds, soak_minutes_per_unit, gear_count
    ) |>
    group_by(id_survey) |>
    summarise(
      seconds = sum(seconds),
      soak_minutes = sum(soak_minutes_per_unit),
      gear_count = sum(gear_count)
    )

  vefmap_cpue <- vefmap |>
    mutate(
      collected = as.integer(collected),
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      catch = collected + observed
    ) |>
    group_by(waterbody, id_site, id_survey, scientific_name, gear_type) |>
    summarise(catch = sum(catch, na.rm = TRUE)) |>
    ungroup() |>
    complete(
      nesting(waterbody, id_site, id_survey, gear_type),
      scientific_name,
      fill = list(catch = 0)
    ) |>
    left_join(effort, by = "id_survey") |>
    filter(!is.na(scientific_name)) |>
    rename(
      effort_s = seconds,
      effort_soak_minutes = soak_minutes,
      effort_gear_count = gear_count
    ) |>
    mutate(
      effort_h = effort_s / 3600,
      effort_specific = ifelse(
        grepl("EF", gear_type), effort_h, effort_gear_count
      ),
      cpue = catch / effort_specific
    ) |>
    select(
      waterbody, id_site, id_survey,
      scientific_name, gear_type,
      effort_h, effort_gear_count, effort_specific,
      cpue
    )

  # order cpue and raw-calculated cpue so we can compare
  cpue <- cpue |>
    ungroup() |>
    select(all_of(colnames(vefmap_cpue))) |>
    arrange(
      waterbody, id_site, id_survey, scientific_name,
      effort_specific,
      cpue
    )
  vefmap_cpue <- vefmap_cpue |>
    arrange(
      waterbody, id_site, id_survey, scientific_name,
      effort_specific,
      cpue
    )

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
  sites <- fetch_table("site") |>
    filter(waterbody == "Campaspe River") |>
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
    target <- dbGetQuery(
      conn = get("DB_ENV", envir = asNamespace("aae.db"))$conn,
      paste0("SELECT * FROM aquatic_data.get_project_data(",
             project_list[i],
             ", 1900, 2099)")
    )
    target <- as_tibble(target)

    # remove surveys without any effort info
    target <- target |>
      filter(
        !(is.na(seconds) & (is.na(soak_minutes_per_unit) | is.na(gear_count))),
        (is.na(seconds) | seconds > 0),
        gear_type != "NONE",
        condition == "FISHABLE"
      )

    # sort and remove time extracted field
    value <- value |>
      arrange(
        id_site, waterbody, site_name, site_desc, id_survey, id_project,
        survey_date, gear_type, id_surveyevent, time_start, condition,
        id_netting, id_sample, id_observation, scientific_name,
        common_name, fork_length_cm, length_cm, collected, observed
      ) |>
      select(-extracted_ts)
    target <- target |>
      select(all_of(colnames(value))) |>
      mutate(
        id_site = as.integer(id_site),
        id_survey = as.integer(id_survey),
        id_surveyevent = as.integer(id_surveyevent),
        id_sample = as.integer(id_sample),
        id_observation = as.integer(id_observation),
        survey_year = as.numeric(survey_year)
      ) |>
      arrange(
        id_site, waterbody, site_name, site_desc, id_survey, id_project,
        survey_date, gear_type, id_surveyevent, time_start, condition,
        id_netting, id_sample, id_observation, scientific_name,
        common_name, fork_length_cm, length_cm, collected, observed
      )

    # compare
    expect_equal(value, target)

  }

  # test that a non-existent project returns an empty table
  value <- fetch_project(85, collect = TRUE)
  expect_equal(nrow(value), 0L)

  # test that a combination of two projects is collected correctly
  project_sub <- project_list[2:5]
  value <- fetch_project(project_sub, collect = TRUE)
  target <- vector("list", length = length(project_sub))
  for (i in seq_along(target)) {
    target[[i]] <- dbGetQuery(
      conn = get("DB_ENV", envir = asNamespace("aae.db"))$conn,
      paste0("SELECT * FROM aquatic_data.get_project_data(",
             project_sub[i],
             ", 1900, 2099)")
    )

  }
  target <- do.call(rbind, target)

  # sort and remove time extracted field
  value <- value |>
    arrange(
      id_site, waterbody, site_name, site_desc, id_survey, id_project,
      survey_date, gear_type, id_surveyevent, time_start, condition,
      id_netting, id_sample, id_observation, scientific_name,
      common_name, fork_length_cm, length_cm, collected, observed
    ) |>
    select(-extracted_ts)
  target <- target |>
    select(all_of(colnames(value))) |>
    mutate(
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      id_surveyevent = as.integer(id_surveyevent),
      id_sample = as.integer(id_sample),
      id_observation = as.integer(id_observation),
      survey_year = as.numeric(survey_year)
    ) |>
    arrange(
      id_site, waterbody, site_name, site_desc, id_survey, id_project,
      survey_date, gear_type, id_surveyevent, time_start, condition,
      id_netting, id_sample, id_observation, scientific_name,
      common_name, fork_length_cm, length_cm, collected, observed
    )
  target <- as_tibble(target)

  # remove surveys without any effort info
  target <- target |>
    filter(
      !(is.na(seconds) & (is.na(soak_minutes_per_unit) | is.na(gear_count))),
      (is.na(seconds) | seconds > 0),
      condition == "FISHABLE"
    )

  # compare
  expect_equal(value, target)

})

test_that("fetch_project and fetch_survey_table return the same surveys", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # fetch the survey table for VEFMAP
  st <- aae.db:::fetch_survey_table(2) |> collect()

  # fetch the survey_event table for VEFMAP, which includes
  #   some known 0 effort surveys
  se <- aae.db:::fetch_survey_event(2) |> collect()

  # fetch the project table
  sp <- fetch_project(2) |>
    distinct(
      id_project, id_site, id_survey, gear_type,
      regime, survey_date, seconds,
      soak_minutes_per_unit, gear_count
    ) |>
    collect()

  # survey events should include some 0 effort surveys
  mismatched <- se |> distinct(id_survey) |> arrange(id_survey) |>
    setdiff(
      st |> distinct(id_survey) |> arrange(id_survey)
    )

  # the project list should not
  matched <- sp |> distinct(id_survey) |> arrange(id_survey) |>
    setdiff(
      st |> distinct(id_survey) |> arrange(id_survey)
    )

  # test this (error on test 2 could be due to clean-up of DB to remove
  #    0 effort surveys)
  expect_equal(nrow(matched), 0L)
  expect_gt(nrow(mismatched), 0L)

})

test_that("fetch_birds works with basic filters on queries or direct", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # download bird data for one site
  value <- fetch_birds(icon_site = "Barmah System", type = "woodland birds")

  # try a query version of the same filter
  target <- fetch_birds() |>
    filter(
      system == "Barmah System",
      type_desc == "woodland birds"
    )

  # rearrange
  value <- value |>
    collect() |>
    arrange(id_survey, id_surveyevent, scientific_name) |>
    select(-extracted_ts)
  target <- target |>
    collect() |>
    arrange(id_survey, id_surveyevent, scientific_name) |>
    select(-extracted_ts)

  # test
  expect_equal(value, target)

})

test_that("fetch_birds returns data for all icon sites", {

  skip_if_offline()
  skip_on_cran()
  skip_on_ci()

  # check if we can connect to the database
  aaedb_connect()
  aaedb_available <- check_aaedb_connection()
  skip_if(!aaedb_available)

  # check each Icon Site in turn
  sys_set <- c(
    "Barmah System",
    "CLLMM System",
    "Gunbower System",
    "Hattah Kulkyne System",
    "Lindsay Mulcra System",
    "Millewa System",
    "Wallpolla System"
  )
  for (i in seq_along(sys_set)) {

    # woodland birds only exist for some sites
    if (grepl("Barmah|Hattah|Millewa", sys_set[i])) {

      # download bird data for each site for woodland birds
      value <- fetch_birds(
        icon_site = sys_set[i], type = "woodland birds", collect = TRUE
      )

      # check something is return and it is just the single system
      expect_gt(nrow(value), 0)
      expect_equal(unique(value$system), sys_set[i])

    }

    # repeat for waterbirds
    value <- fetch_birds(
      icon_site = sys_set[i], type = "waterbirds", collect = TRUE
    )

    # check something is return and it is just the single system
    expect_gt(nrow(value), 0)
    expect_equal(unique(value$system), sys_set[i])

  }

})
