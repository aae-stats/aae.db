# collection of functions that support the `fetch_` methods
#' @importFrom dplyr all_of filter group_by inner_join left_join
#'   mutate rename select summarise
#' @importFrom rlang `!!`

# internal function to fetch survey event info
fetch_survey_event <- function(project_id, ...) {

  # grab survey event info
  survey_event_info <- fetch_table("site", ...) |>
    dplyr::select(id_site, waterbody, site_desc) |>
    dplyr::inner_join(
      fetch_table("survey", ...) |>
        dplyr::select(
          id_site, id_survey, id_project, sdate, gear_type, released
        ),
      by = "id_site"
    ) |>
    dplyr::mutate(syear = year(sdate)) |>
    dplyr::rename(
      survey_year = syear,
      survey_date = sdate
    ) |>
    dplyr::left_join(
      fetch_table("survey_event", ...) |>
        dplyr::select(
          id_survey, id_surveyevent, time_start, time_finish, condition
        ),
      by = "id_survey"
    ) |>
    dplyr::left_join(
      fetch_table("site_project_lu", ...) |>
        dplyr::select(id_site, id_project, site_name),
      by = c("id_site", "id_project")
    ) |>
    dplyr::filter(
      id_project %in% project_id,
      released
    ) |>
    dplyr::mutate(
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      id_surveyevent = as.integer(id_surveyevent)
    ) |>
    dplyr::select(-released)

}

# internal function to add electrofishing survey metadata
add_electro <- function(x, ...) {
  x |>
    dplyr::left_join(
      fetch_table("electro", ...) |>
        dplyr::select(id_surveyevent, seconds),
      by = "id_surveyevent"
    )
}

# internal function to add netting metadata
add_netting <- function(x, ...) {
  x |>
    dplyr::left_join(
      fetch_table("netting", ...) |>
        dplyr::select(
          id_surveyevent, id_netting, soak_minutes_per_unit, gear_count
        ),
      by = "id_surveyevent"
    )
}

# internal function to fetch taxon lookup table
fetch_taxon_lu <- function(...) {
  fetch_table("taxon_lu", ...) |>
    dplyr::select(id_taxon, scientific_name, common_name)
}

# internal function to fetch info on collected taxa
fetch_collected <- function(survey_event, taxon_lu, ...) {

  # grab collected table from aaedb and add empty fields
  #   for compatibility with observation table

  fetch_table("sample", ...) |>
    dplyr::select(
      id_surveyevent,
      id_sample,
      id_taxon,
      fork_length_cm,
      length_cm,
      weight_g,
      collected
    ) |>
    dplyr::mutate(
      id_observation = as.integer(NA),
      observed = as.integer(NA),
      id_surveyevent = as.integer(id_surveyevent),
      id_observation = as.integer(id_observation),
      id_sample = as.integer(id_sample)
    ) |>
    dplyr::inner_join(
      survey_event |> dplyr::select(id_surveyevent),
      by = "id_surveyevent"
    ) |>
    dplyr::inner_join(taxon_lu, by = "id_taxon")

}

# internal function to fetch info on observed taxa
fetch_observed <- function(survey_event, taxon_lu, taxa_collected, ...) {

  # grab observation table from aaedb and add empty fields
  #   for compatibility with collected table
  fetch_table("observation", ...) |>
    dplyr::select(id_surveyevent, id_observation, id_taxon, count) |>
    dplyr::rename(observed = count) |>
    dplyr::mutate(
      id_sample = as.integer(NA),
      fork_length_cm = as.numeric(NA),
      length_cm = as.numeric(NA),
      weight_g = as.numeric(NA),
      collected = as.integer(NA),
      id_surveyevent = as.integer(id_surveyevent),
      id_observation = as.integer(id_observation)
    ) |>
    dplyr::left_join(
      survey_event |> dplyr::select(id_surveyevent),
      by = "id_surveyevent"
    ) |>
    dplyr::inner_join(taxon_lu, by = "id_taxon") |>
    dplyr::select(dplyr::all_of(colnames(taxa_collected)))

}

# internal function to download a survey table for a project or
#   set of projects
fetch_survey_table <- function(project_id, ...) {

  # grab site, survey, and survey_event information
  survey_table <- fetch_table("site", ...) |>
    dplyr::select(id_site) |>
    dplyr::left_join(
      fetch_table("survey", ...) |>
        dplyr::select(id_site, id_survey, id_project, gear_type, sdate, released),
      by = "id_site"
    ) |>
    dplyr::left_join(
      fetch_table("survey_event", ...) |>
        dplyr::select(id_survey, id_surveyevent, condition),
      by = "id_survey"
    )

  # add electro information (seconds power on)
  survey_table <- survey_table |> add_electro(...)

  # filter to surveys that actually collected data
  survey_table <- survey_table |>
    dplyr::filter(
      id_project %in% project_id,
      grepl(!!"EF", gear_type),
      released,
      condition == !!"FISHABLE"
    ) |>
    dplyr::select(-released, -condition)

  # calculate total electro seconds for each survey (sum over survey events)
  survey_table <- survey_table |>
    dplyr::group_by(id_project, id_site, id_survey, gear_type, sdate) |>
    dplyr::summarise(seconds = sum(seconds, na.rm = TRUE))

  # cast 64-bit ints to 32-bit, drop dodgy projects or seconds estimates,
  #   and return an ordered result
  survey_table <- survey_table |>
    dplyr::mutate(
      id_project = as.integer(id_project),
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      seconds = as.integer(seconds)
    ) |>
    dplyr::filter(!is.na(seconds), seconds > 0, id_project != -99)

  # return unevaluated query
  survey_table

}
