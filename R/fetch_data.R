#' @name fetch_data
#'
#' @title Query the AAEDB and return a query object to be manipulated
#'   and evaluated
#'
#' @export
#'
#' @importFrom dplyr all_of distinct filter full_join group_by
#'   inner_join left_join mutate rename select summarise tbl
#'   ungroup union_all pull join_by
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
#' @param icon_site the TLM Icon Site for which data are required. Applicable
#'    only to TLM bird data sets queried with \code{fetch_birds}
#' @param type the TLM bird survey type. One of \code{woodland birds} or
#'    \code{waterbirds}. Applicable only to TLM bird data sets queried with
#'    \code{fetch_birds}
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
#'   \code{fetch_cpue} and \code{fetch_birds} are wrappers to calculate full
#'   CPUE data sets for fish or full count data sets for birds, respectively.
#'   These functions create full survey tables, which accounts for zero
#'   catch/count records in surveyed sites where a particular species was
#'   not recorded. These records are excluded from queries to
#'   \code{fetch_project}.
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
#'     by = join_by(id_site)
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
#' # download TLM bird data for the Barmah Icon Site woodland birds
#' birds <- fetch_birds(
#'   icon_site = "Barmah System",
#'   type = "woodland birds"
#' ) |>
#'   collect()
#'
#' # and download species or habitat info associated with these surveys
#' species_info <- birds |> fetch_species_info()
#' habitat <- birds |> fetch_habitat_info()
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

  # filter out missing electro/netting events before catch data are calculated
  #   (missing is determined by no effort information in electro and netting
  #    tables)
  survey_event <- survey_event |>
    filter(
      !((is.na(soak_minutes_per_unit) | is.na(gear_count)) &
          is.na(seconds))
    )

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
      by = dplyr::join_by(id_surveyevent)
    ) |>
    dplyr::filter(
      seconds > 0 | soak_minutes_per_unit > 0 | gear_count > 0
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

  # filter out missing electro/netting events before catch data are calculated
  #   (missing is determined by no effort information in electro and netting
  #    tables)
  survey_event <- survey_event |>
    filter(
      !(is.na(seconds) & is.na(soak_minutes_per_unit) & is.na(gear_count))
    )

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
      by = dplyr::join_by(id_surveyevent)
    ) |>
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
      regime,
      survey_date,
      survey_year,
      gear_type,
      scientific_name
    ) |>
    dplyr::summarise(catch = sum(catch, na.rm = TRUE)) |>
    dplyr::ungroup()

  # create a full survey x species table, which will allow us to fill
  #   unrecorded species with zero valuesc
  if (!is.null(project_id)) {

    # grab with project_id if available
    survey_table <- fetch_survey_table(project_id, ...)

  } else {

    # otherwise use the specified reporting system
    #  (all_projects and system_table are defined above)
    survey_table <- fetch_survey_table(all_projects, ...) |>
      dplyr::filter(id_site %in% !!system_table$id_site)

  }

  # expand survey table and remove NA species
  #    (these are needed to keep No Fish surveys in the survey table)
  survey_table_expanded <- survey_table |>
    dplyr::distinct(id_survey) |>
    dplyr::left_join(
      catch |> dplyr::distinct(id_survey, scientific_name),
      by = dplyr::join_by(id_survey)
    ) |>
    tidyr::complete(
      id_survey,
      scientific_name
    ) |>
    dplyr::filter(!is.na(scientific_name))

  survey_table_expanded <- survey_table_expanded |>
    dplyr::left_join(
      survey_table |>
        dplyr::distinct(
          id_project, id_site, id_survey,
          gear_type, sdate,
          regime,
          seconds, soak_minutes, gear_count
        ),
      by = dplyr::join_by(id_survey)
    ) |>
    dplyr::rename(survey_date = sdate)

  # connect catch data to survey table and calculate CPUE
  cpue <- survey_table_expanded |>
    dplyr::left_join(
      catch |>
        dplyr::distinct(
          id_project,
          waterbody,
          id_site,
          site_name,
          site_desc,
          id_survey,
          regime,
          survey_date,
          survey_year,
          gear_type
        ),
      by = dplyr::join_by(
        id_project,
        id_site,
        id_survey,
        gear_type,
        survey_date,
        regime
      )
    ) |>
    dplyr::rename(
      effort_s = seconds,
      effort_soak_minutes = soak_minutes,
      effort_gear_count = gear_count
    ) |>
    dplyr::left_join(
      catch |> dplyr::select(id_survey, scientific_name, catch),
      by = dplyr::join_by(id_survey, scientific_name)
    ) |>
    dplyr::mutate(
      catch = ifelse(is.na(catch), 0, catch),
      effort_h = effort_s / 3600,
      effort_specific = ifelse(
        grepl("EF", gear_type), effort_h, effort_gear_count
      ),
      cpue = catch / effort_specific
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

#' @rdname fetch_data
#'
#' @export
#'
fetch_birds <- function(icon_site = NULL, type = NULL, collect = FALSE, ...) {

  # connect to database if required
  new_connection <- connect_if_required("fetch_project", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # grab survey event table
  survey_event <- fetch_table("survey_event", schema = "birds", ...)

  # pull out relevant fields
  survey_event <- survey_event |>
    dplyr::select(
      id_surveyevent,
      id_survey,
      time_start,
      time_finish,
      condition
    )

  # grab info on collected and observed taxa
  taxon_lu <- aae.db:::fetch_taxon_lu(...)
  taxa_observed <- fetch_table("observation", "birds", ...)

  # combine everything into a single table and keep all gears
  survey_event <- survey_event |>
    dplyr::left_join(
      taxa_observed,
      by = dplyr::join_by(id_surveyevent)
    ) |>
    dplyr::left_join(taxon_lu, by = dplyr::join_by(id_taxon)) |>
    select(
      id_surveyevent,
      id_survey,
      time_start,
      time_finish,
      condition,
      id_observation,
      id_taxon,
      scientific_name,
      common_name,
      count,
      count_qualifier,
      count_accuracy,
      age_class,
      breeding,
      micro_habitat,
      activity,
      sex,
      notes
    )

  # create a full survey x species table, which will allow us to fill
  #   unrecorded species with zero valuesc
  survey_table <- fetch_table("survey", "birds", ...)

  # add some useful info
  survey_table <- survey_table |>
    dplyr::left_join(
      fetch_table("survey_type_lu", "birds", ...) |>
        dplyr::select(survey_type_id, type_desc),
      by = dplyr::join_by(survey_type_id)
    ) |>
    dplyr::left_join(
      fetch_table("site", ...) |>
        dplyr::select(id_site, waterbody, site_name, site_desc),
      by = dplyr::join_by(id_site)
    ) |>
    dplyr::left_join(
      fetch_table("site_system", ...),
      by = dplyr::join_by(id_site)
    )

  # filter to target system and survey type
  if (!is.null(icon_site)) {
    sys_list <- survey_table |>
      dplyr::distinct(system) |>
      collect() |>
      dplyr::pull(system)
    if (!all(icon_site %in% sys_list)) {
      stop(
        paste0(
          "icon_site must be one of `",
          paste(sys_list[-length(sys_list)], collapse = "`, `"),
          "`, or `",
          sys_list[length(sys_list)],
          "`"
        ),
        call. = FALSE
      )
    }
    survey_table <- survey_table |>
      dplyr::filter(system %in% !!icon_site)
  }
  if (!is.null(type)) {
    if (!all(type %in% c("waterbirds", "woodland birds"))) {
      stop(
        "type must be one of `waterbirds` or `woodland birds`",
        call. = FALSE
      )
    }
    survey_table <- survey_table |>
      dplyr::filter(type_desc %in% !!type)
  }

  # expand survey table
  survey_table_expanded <- survey_table |>
    dplyr::filter(released) |>
    dplyr::distinct(id_survey, system, type_desc) |>
    dplyr::left_join(
      survey_event |>
        dplyr::distinct(
          id_survey, id_surveyevent, scientific_name, common_name, condition
        ),
      by = dplyr::join_by(id_survey)
    ) |>
    dplyr::filter(condition == "SURVEYABLE") |>
    dplyr::mutate(
      scientific_name = ifelse(
        is.na(scientific_name),
        "No birds",
        scientific_name
      )
    ) |>
    dplyr::group_by(system, type_desc) |>
    tidyr::complete(
      tidyr::nesting(id_survey, id_surveyevent, condition),
      tidyr::nesting(scientific_name, common_name)
    ) |>
    dplyr::ungroup()

  # add counts, fill NA with zero
  counts <- survey_table_expanded |>
    dplyr::left_join(
      survey_event |>
        dplyr::select(-condition, -id_observation, -id_taxon, -common_name),
      by = dplyr::join_by(id_survey, id_surveyevent, scientific_name)
    ) |>
    dplyr::left_join(
      survey_table |>
        dplyr::select(
          id_survey,
          id_site,
          sdate,
          sdate_accuracy,
          survey_method,
          regime,
          site_coverage_perc,
          waterbody,
          site_name,
          site_desc
        ),
      by = dplyr::join_by(id_survey)
    )

  # convert int64 to interger
  counts <- counts |>
    dplyr::mutate(
      count = as.integer(count),
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      id_surveyevent = as.integer(id_surveyevent)
    )

  # tidy this output and add a time stamp
  counts <- counts |>
    dplyr::rename(
      survey_date = sdate,
      survey_date_accuracy = sdate_accuracy
    ) |>
    dplyr::mutate(
      extracted_ts = dplyr::sql("timezone('Australia/Melbourne'::text, now())")
    ) |>
    dplyr::select(dplyr::all_of(count_return_cols)) |>
    dplyr::filter(scientific_name != "No birds")

  # collect data if required
  if (collect)
    counts <- counts |> collect()

  # and return
  counts

}

# internal list of variables to be returned from database for individual records
survey_event_return_cols <- c(
  "id_site",
  "waterbody",
  "site_name",
  "site_desc",
  "id_survey",
  "regime",
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
  "regime",
  "id_project",
  "survey_date",
  "survey_year",
  "gear_type",
  "effort_s",
  "effort_h",
  "effort_soak_minutes",
  "effort_gear_count",
  "effort_specific",
  "scientific_name",
  "catch",
  "cpue",
  "extracted_ts"
)

# internal list of variables to be returned from database for count records
count_return_cols <- c(
  "system",
  "waterbody",
  "site_name",
  "site_desc",
  "regime",
  "survey_date",
  "survey_date_accuracy",
  "time_start",
  "time_finish",
  "type_desc",
  "survey_method",
  "scientific_name",
  "common_name",
  "count",
  "count_qualifier",
  "count_accuracy",
  "age_class",
  "breeding",
  "micro_habitat",
  "activity",
  "site_coverage_perc",
  "sex",
  "notes",
  "id_site",
  "id_survey",
  "id_surveyevent",
  "extracted_ts"
)
