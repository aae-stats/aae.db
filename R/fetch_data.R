#' @name fetch_data
#'
#' @title Query the AAEDB and return a query object to be manipulated
#'   and evaluated
#'
#' @export
#'
#' @importFrom dplyr all_of distinct filter full_join group_by
#'   inner_join left_join mutate rename select summarise tbl
#'   ungroup union_all
#' @importFrom dbplyr in_schema sql
#' @importFrom tidyr complete nesting
#' @importFrom rlang `!!` sym
#'
#' @param x a character specifying an individual table in the AAEDB
#' @param schema schema in which \code{x} is found. Defaults to
#'   \code{"aquatic_data"}
#' @param project_id an integer specifying an individual AAE project.
#'    Any integer values will be accepted but non-existent projects will
#'    return an empty query. Use \code{list_projects} to see a list of all
#'    current projects
#' @param reporting_system a character specifying an individual reporting
#'    system, which may encompass multiple waterbodies. Reporting systems
#'    are specified for only a subset of waterbodies; will return an empty
#'    table if a reporting system is not specified. Use \code{list_systems}
#'    to see a list of all current systems
#' @param collect logical: should a query be executed (\code{TRUE}) or
#'   evaluated lazily (\code{FALSE}, the default)
#' @param criterion \code{list} containing `var`, `lower`, and `upper` elements
#'   specifying bounds on `var`. Allows complex filtering such as restricting
#'   CPUE estimates to a specific length or weight range
#' @param \dots additional arguments passed to \link[dbplyr]{tbl_sql}
#'
#' @description \code{fetch_table} and \code{fetch_project}
#'   represent two ways to interact with the AAEDB.
#'   \code{fetch_table} provides access to prepared tables in
#'   the database and \code{fetch_project} selects data for an
#'   individual AAE project.
#'
#'   All functions require credentials to access the AAEDB, as well
#'   as a appropriate VPN connection. If making multiple queries,
#'   it can be easier to connect once to the AAEDB rather than repeatedly
#'   connecting (and disconnecting). This is possible with the
#'   \link[aae.db]{aaedb_connect} function.
#'
#'   Update for version 0.1.0: the \code{fetch_} functions now return an
#'   unevaluated query rather than a full data table. This allows further
#'   filtering or changes to the query using \code{dplyr} methods prior to
#'   downloading the data. This is especially useful when downloading a subset
#'   of a much larger data sets. All three methods will remain available in
#'   future versions but may be renamed to better reflect their intended use.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # dplyr methods used below
#' library(dplyr)
#'
#' # set up a query that includes the full VEFMAP data set
#' vefmap <- fetch_project(2)
#'
#' # can manipulate and filter this query with dplyr methods
#' vefmap <- vefmap |>
#'   filter(
#'     waterbody == "Campaspe River",
#'     scientific_name == "Maccullochella peelii"
#'   )
#'
#' # evaluate this query with collect
#' vefmap <- vefmap |> collect()
#'
#' # process a nested query using raw tables from the database
#' #   and `dplyr` methods
#' site_data <- fetch_table("site")
#' survey_data <- fetch_table("survey")
#' survey_info_dplyr <- site_data |>
#'   left_join(
#'     survey_data |> distinct(id_site, id_project),
#'     by = "id_site"
#'   ) |>
#'   filter(grepl("ovens", waterbody, ignore.case = TRUE)) |>
#'   distinct(waterbody, id_project) |>
#'   arrange(waterbody, id_project) |>
#'   collect()
#'
#' # and grab information for individual projects
#' ovens_data <- fetch_project(9)
#' ovens_data <- ovens_data |> collect()
#'
#' # subset this to 2015-2017 surveys
#' ovens_data <- fetch_project(9)
#' ovens_data <- ovens_data |>
#'   filter(survey_year %in% c(2015:2017)) |>
#'   collect()
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' #   when all queries and evaluation is complete
#' # aaedb_disconnect()
#'
#' @rdname fetch_data
fetch_table <- function(x, schema = "aquatic_data", collect = FALSE, ...) {

  # connect to database if required
  new_connection <- connect_if_required("fetch_table", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # view flat file from specified schema
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(x)),
    ...
  )

  # collect data if required
  if (collect)
    out <- out |> collect()

  # return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_project <- function(
    project_id = NULL, reporting_system = NULL, collect = FALSE, ...
) {

  # one of project_id or reporting_system must be specified
  if (is.null(project_id) & is.null(reporting_system)) {
    stop("project_id or reporting_system must be specified", call. = FALSE)
  }

  # warn if both project_id and reporting_system are specified; default to project
  if (!is.null(project_id) & !is.null(reporting_system)) {
    warning(
      "project_id and reporting_system are both specified;",
      " returning results for project_id only"
    )
  }

  # connect to database if required
  new_connection <- connect_if_required("fetch_project", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # grab survey event table from project_id if specified
  if (!is.null(project_id)) {

    # project_id must be an integer/integer vector
    if (!all((project_id %% 1) == 0)) {
      stop(
        "project_id must be an integer or vector of ",
        "integers (see ?fetch_project for details)",
        call. = FALSE
      )
    }

    # grab the survey table
    survey_event <- fetch_survey_event(project_id, ...) |>
      add_electro(...) |>
      add_netting(...)

  } else {

    # and grab target systems for all projects otherwise
    all_projects <- fetch_table("project_lu") |>
      dplyr::filter(id_project > 0) |>
      dplyr::distinct(id_project) |>
      collect() |>
      dplyr::pull(id_project)

    # work out relevant sites
    system_table <- fetch_table("site_system") |>
      dplyr::filter(system %in% !!reporting_system) |>
      collect()

    # grab data
    survey_event <- fetch_survey_event(all_projects, ...) |>
      dplyr::filter(id_site %in% !!system_table$id_site) |>
      add_electro(...) |>
      add_netting(...)
  }

  # grab info on collected and observed taxa
  taxon_lu <- fetch_taxon_lu(...)
  taxa_collected <- fetch_collected(survey_event, taxon_lu, ...)
  taxa_observed <- fetch_observed(survey_event, taxon_lu, taxa_collected, ...)
  taxa_all <- taxa_collected |> dplyr::union_all(taxa_observed)

  # combine everything into a single table
  out <- survey_event |>
    dplyr::left_join(
      taxa_all |> dplyr::select(
        id_surveyevent,
        id_sample,
        id_observation,
        scientific_name,
        common_name,
        fork_length_cm,
        length_cm,
        weight_g,
        collected,
        observed
      ),
      by = "id_surveyevent"
    ) |>
    dplyr::mutate(
      extracted_ts = dplyr::sql("timezone('Australia/Melbourne'::text, now())")
    ) |>
    dplyr::select(dplyr::all_of(survey_event_return_cols))

  # collect data if required
  if (collect)
    out <- out |> collect()

  # and return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_cpue <- function(
    project_id = NULL,
    reporting_system = NULL,
    collect = FALSE,
    criterion = NULL, ...
) {

  # one of project_id or reporting_system must be specified
  if (is.null(project_id) & is.null(reporting_system)) {
    stop("project_id or reporting_system must be specified", call. = FALSE)
  }

  # warn if both project_id and reporting_system are specified; default to project
  if (!is.null(project_id) & !is.null(reporting_system)) {
    warning(
      "project_id and reporting_system are both specified;",
      " returning results for project_id only"
    )
  }

  # connect to database if required
  new_connection <- connect_if_required("fetch_project", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # grab survey event table from project_id if specified
  if (!is.null(project_id)) {

    # project_id must be an integer/integer_vector
    if (!all((project_id %% 1) == 0)) {
      stop(
        "project_id must be an integer or vector of ",
        "integers (see ?fetch_project for details)",
        call. = FALSE
      )
    }

    # grab survey table
    survey_event <- fetch_survey_event(project_id, ...) |>
      add_electro(...) |>
      add_netting(...)

  } else {

    # and grab target reporting_system for all projects otherwise
    all_projects <- fetch_table("project_lu") |>
      dplyr::filter(id_project > 0) |>
      dplyr::distinct(id_project) |>
      collect() |>
      dplyr::pull(id_project)

    # work out relevant sites
    system_table <- fetch_table("site_system") |>
      dplyr::filter(system %in% !!reporting_system) |>
      collect()

    # grab data
    survey_event <- fetch_survey_event(all_projects, ...) |>
      dplyr::filter(id_site %in% !!system_table$id_site) |>
      add_electro(...) |>
      add_netting(...)

  }

  # grab info on collected and observed taxa
  taxon_lu <- fetch_taxon_lu(...)
  taxa_collected <- fetch_collected(survey_event, taxon_lu, ...)
  taxa_observed <- fetch_observed(survey_event, taxon_lu, taxa_collected, ...)
  taxa_all <- taxa_collected |> dplyr::union_all(taxa_observed)

  # combine everything into a single table and keep all gears
  survey_event <- survey_event |>
    dplyr::left_join(
      taxa_all |> dplyr::select(
        id_surveyevent,
        scientific_name,
        fork_length_cm,
        length_cm,
        weight_g,
        collected,
        observed
      ),
      by = "id_surveyevent"
    ) |>
    dplyr::filter(condition == !!"FISHABLE") |>
    dplyr::select(-time_start, -time_finish, -condition)

  # apply criterion if needed (allows setting a subset of rows to zero
  #   catch based on a specified function and variables)
  survey_event <- survey_event |>
    dplyr::mutate(criterion = TRUE)
  if (!is.null(criterion)) {
    survey_event <- survey_event |>
      mutate(
        criterion = (!!rlang::sym(criterion$var) > !!criterion$lower) &
          (!!rlang::sym(criterion$var) <= !!criterion$upper)
      )
  }

  # calculate total catch per survey
  catch <- survey_event |>
    dplyr::mutate(
      collected = ifelse(is.na(collected), 0, collected),
      observed = ifelse(is.na(observed), 0, observed),
      collected = ifelse(criterion, collected, 0),
      observed = ifelse(criterion, observed, 0),
      catch = collected + observed
    ) |>
    dplyr::group_by(
      id_project,
      waterbody,
      id_site,
      site_name,
      site_desc,
      id_survey,
      survey_date,
      survey_year,
      gear_type,
      scientific_name
    ) |>
    dplyr::summarise(catch = sum(catch, na.rm = TRUE)) |>
    dplyr::ungroup()

  # create a full survey x species table, which will allow us to fill
  #   unrecorded species with zero values
  if (!is.null(project_id)) {

    # grab with project_id if available
    survey_table <- fetch_survey_table(project_id, ...)

  } else {

    # otherwise use the specified reporting system
    #  (all_projects and system_table are defined above)
    survey_table <- fetch_survey_table(all_projects, ...) |>
      dplyr::filter(id_site %in% !!system_table$id_site)

  }

  # expand survey table
  survey_table <- survey_table |>
    dplyr::left_join(
      catch |> dplyr::distinct(id_survey, scientific_name),
      by = "id_survey"
    ) |>
    dplyr::ungroup() |>
    tidyr::complete(
      tidyr::nesting(
        id_project, id_site, id_survey, gear_type,
        seconds, soak_minutes, gear_count
      ),
      scientific_name
    ) |>
    dplyr::filter(!is.na(scientific_name))

  # connect catch data to survey table and calculate CPUE
  cpue <- survey_table |>
    dplyr::left_join(
      catch |>
        dplyr::distinct(
          id_project,
          waterbody,
          id_site,
          site_name,
          site_desc,
          id_survey,
          survey_date,
          survey_year,
          gear_type
        ),
      by = c("id_project", "id_site", "id_survey", "gear_type")
    ) |>
    dplyr::rename(
      effort_s = seconds,
      effort_soak_minutes = soak_minutes,
      effort_gear_count = gear_count
    ) |>
    dplyr::left_join(
      catch |> dplyr::select(id_survey, scientific_name, catch),
      by = c("id_survey", "scientific_name")
    ) |>
    dplyr::mutate(
      catch = ifelse(is.na(catch), 0, catch),
      effort_h = effort_s / 3600,
      cpue = catch / effort_h
    )

  # tidy this output and add a time stamp
  cpue <- cpue |>
    dplyr::mutate(
      extracted_ts = dplyr::sql("timezone('Australia/Melbourne'::text, now())")
    ) |>
    dplyr::select(dplyr::all_of(cpue_return_cols)) |>
    dplyr::filter(!is.na(scientific_name))

  # collect data if required
  if (collect)
    cpue <- cpue |> collect()

  # and return
  cpue

}

# internal list of variables to be returned from database for individual records
survey_event_return_cols <- c(
  "id_site",
  "waterbody",
  "site_name",
  "site_desc",
  "id_survey",
  "id_project",
  "survey_date",
  "survey_year",
  "gear_type",
  "id_surveyevent",
  "time_start",
  "time_finish",
  "condition",
  "seconds",
  "id_netting",
  "soak_minutes_per_unit",
  "gear_count",
  "id_sample",
  "id_observation",
  "scientific_name",
  "common_name",
  "fork_length_cm",
  "length_cm",
  "weight_g",
  "collected",
  "observed",
  "extracted_ts"
)

# internal list of variables to be returned from database for CPUE records
cpue_return_cols <- c(
  "id_site",
  "waterbody",
  "site_name",
  "site_desc",
  "id_survey",
  "id_project",
  "survey_date",
  "survey_year",
  "gear_type",
  "effort_s",
  "effort_h",
  "effort_soak_minutes",
  "effort_gear_count",
  "scientific_name",
  "catch",
  "cpue",
  "extracted_ts"
)
