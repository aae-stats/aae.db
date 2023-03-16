#' @name fetch_data
#'
#' @title Query the AAEDB and return a query object to be manipulated
#'   and downloaded
#'
#' @export
#'
#' @importFrom rstudioapi askForPassword
#' @importFrom methods hasArgs
#' @importFrom all_of arrange dplyr filter inner_join left_join mutate
#'   rename select tbl
#' @importFrom dbplyr sql in_schema
#'
#' @param x a character specifying an individual table in the AAEDB
#' @param schema schema in which \code{x} is found. Defaults to
#'   \code{"aquatic_data"}
#' @param query a character specifying a SQL query
#' @param project_id an integer specifying an individual AAE project
#'   (1 - Snags, 2 - VEFMAP, 4 - NFRC, 6 - Kiewa Ovens, 7 - SRA,
#'    8 - Southern Basins, 9 - Ovens Demo Reaches, 10 - King Parrot
#'    Creek Macquarie Perch, 11 - Lower Goulburn Projects, 12 - Hughes
#'    Creek Macquarie Perch, 13 - Seven Creeks Macquarie Perch,
#'    14 - Index of Estuarine Condition, 15 - LTIM Lower Goulburn,
#'    16 - IVT Broken Creek)
#' @param collect logical: should a query be executed (\code{TRUE}) or
#'   evaluated lazily (\code{FALSE}, the default)
#' @param x a collected query (table), used to filter waterbodies, sites
#'   and projects in \code{fetch_site_info} or species scientific and
#'   common names in \code{fetch_species_info}
#' @param pattern a regex pattern used to filter waterbodies in
#'   \code{fetch_site_info} or scientific names in \code{fetch_species_info}
#' @param taxon taxonomic group for which data are requested in
#'   \code{fetch_species_info}. Defaults to "Fish". See examples to extract
#'   a list of all possible taxonomic groups
#' @param primary_discipline discipline for which data are requested in
#'   \code{fetch_species_info}. Defaults to "Aquatic fauna" but can take
#'   values of "Aquatic fauna", "Flora", "Aquatic invertebrates",
#'   "Terrestrial fauna", and "Marine"
#' @param \dots additional arguments passed to \link[dbplyr]{tbl_sql} for
#'   \code{fetch_table} and \code{fetch_query} (ignored in
#'   \code{fetch_project})
#'
#' @description \code{fetch_table}, \code{fetch_query}, and
#'   \code{fetch_project} represent three ways to interact with the
#'   AAEDB. \code{fetch_table} provides access to prepared tables in
#'   the database, \code{fetch_query} allows users to compute custom
#'   queries, and \code{fetch_project} selects data for an
#'   individual AAE project.
#'
#'   All functions require credentials to access the AAEDB, as well
#'   as a appropriate VPN connection. If making multiple queries,
#'   it can be easier to connect once to the AAEDB rather than repeatedly
#'   connecting (and disconnecting). This is possible with the
#'   \link[aae.db]{aaedb_connect} function.
#'
#'   \code{fetch_query} can be used to download anything
#'   you would download with \code{fetch_table} or \code{fetch_project}.
#'   The benefits of \code{fetch_table} and \code{fetch_project} are in
#'   providing access to prepared tables containing commonly used variables
#'   for analyses of fish data. The benefit of \code{fetch_query} is in
#'   allowing custom queries. This may be especially useful when working
#'   with large spatial data sets.
#'
#'   Update: the \code{fetch_} functions now return an unevaluated query
#'   rather than a full data table. This allows further filtering or
#'   changes to the query using \code{dplyr} methods prior to downloading
#'   the data. This is especially useful when downloading a subset of a
#'   much larger data sets. All three methods will remain available in
#'   future versions but may be renamed to better reflect their intended
#'   use.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # dplyr methods used below
#' library(dplyr)
#'
#' # set up a query that includes the full flat VEFMAP data set
#' vefmap <- fetch_table("v_vefmap_only_flat_data")
#'
#' # can manipulate and filter this query with dplyr methods
#' vefmap <- vefmap %>%
#'   filter(
#'     waterbody == "Campaspe River",
#'     scientific_name == "Maccullochella peelii"
#'   )
#'
#' # evaluate this query with collect
#' vefmap <- vefmap %>% collect()
#'
#' # fetch information on the sites in a data set
#' vefmap_site_info <- fetch_site_info(vefmap)
#'
#' # "spatialise" this information
#' library(sf)
#' vefmap_site_info <- vefmap_site_info %>%
#'   filter(!is.na(geom_pnt)) %>%
#'   collect()
#' vefmap_sf <- vefmap_site_info %>%
#'   st_set_geometry(st_as_sfc(vemfap_site_info$geom_pnt))
#'
#' # or for all sites (optionally matching a regex expression)
#' murray_site_info <- fetch_site_info(pattern = "^Murray")
#'
#' # extract information on the species in a data set
#' fetch_species_info(vefmap)
#'
#' # or for all species with scientific names matching a pattern
#' fetch_species_info(pattern = "Maccull")
#'
#' # list all taxonomic groups for use in fetch_species_info
#' fetch_table("taxon_lu", collect = FALSE) %>%
#'   select(taxon_type) %>%
#'   collect() %>%
#'   pull(taxon_type) %>%
#'   unique()
#'
#' # and download data for one group
#' fetch_species_info(taxon = "Aquatic invertebrates")
#'
#' # process a simple SQL query to list all projects with data from the
#' #   Ovens river
#' survey_info <- fetch_query(
#'   "SELECT waterbody, id_project
#'      FROM aquatic_data.site a LEFT JOIN aquatic_data.survey b
#'      ON a.id_site = b.id_site
#'      WHERE lower(waterbody) LIKE 'ovens%'
#'      GROUP BY waterbody, id_project
#'      ORDER by waterbody, id_project",
#'   collect = FALSE
#' )
#' survey_info <- survey_info %>% collect()
#'
#' # process this same query using dplyr methods
#' site_data <- fetch_table("site")
#' survey_data <- fetch_table("survey")
#' survey_info_dplyr <- site_data %>%
#'   left_join(
#'     survey_data %>% distinct(id_site, id_project),
#'     by = "id_site"
#'   ) %>%
#'   filter(grepl("ovens", waterbody, ignore.case = TRUE)) %>%
#'   distinct(waterbody, id_project) %>%
#'   arrange(waterbody, id_project) %>%
#'   collect()
#'
#' # and grab information for individual projects
#' ovens_data <- fetch_project(9)
#' ovens_data <- ovens_data %>% collect()
#'
#' # repeat this for a subset of years
#' ovens_data <- fetch_project(9, start = 2015, end = 2017)
#' ovens_data <- ovens_data %>% collect()
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
    out <- out %>% collect()

  # return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_query <- function(query, collect = FALSE, ...) {

  # query must be a function or string
  if (!is.character(query))
    stop("query must be a SQL query (string)", call. = FALSE)

  # check that the query isn't a vector
  if (length(query) > 1)
    stop("SQL query has length > 1 but must be a string", .call = FALSE)

  # connect to database if required
  new_connection <- connect_if_required("fetch_query", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # grab query, assuming it's a string SQL query
  out <- dplyr::tbl(DB_ENV$conn, dbplyr::sql(query), ...)

  # collect data if required
  if (collect)
    out <- out %>% collect()

  # return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_project <- function(project_id, collect = FALSE, ...) {

  # query must be a function or string
  if (!project_id %in% c(1, 2, 4, 6:16)) {
    stop(
      "project_id must be a valid integer (see ?fetch_project for details)",
      call. = FALSE
    )
  }

  # connect to database if required
  new_connection <- connect_if_required("fetch_project", collect = collect)

  # and kick this connection on exit if it's new and the query is executed
  if (collect & new_connection)
    on.exit(aaedb_disconnect())

  # grab survey event table
  survey_event <- fetch_survey_event(project_id) %>%
    add_electro(survey_event) %>%
    add_netting(survey_event)

  # grab info on collected and observed taxa
  taxon_lu <- fetch_taxon_lu()
  taxa_collected <- fetch_collected(survey_event, taxon_lu)
  taxa_observed <- fetch_observed(survey_event, taxon_lu)
  taxa_all <- taxa_collected %>% union_all(taxa_observed)

  # combine everything into a single table
  out <- survey_event %>%
    dplyr::left_join(
      taxa_all %>% dplyr::select(
        id_surveyevent,
        id_sample,
        id_observation,
        id_taxon,
        scientific_name,
        common_name,
        fork_length_cm,
        length_cm,
        weight_g,
        collected,
        observed
      ),
      by = "id_surveyevent"
    ) %>%
    dplyr::arrange(
      id_site, id_survey, id_surveyevent, scientific_name, common_name
    ) %>%
    dplyr::mutate(
      extracted_ts = dplyr::sql("timezone('Australia/Melbourne'::text, now())")
    ) %>%
    dplyr::select(dplyr::all_of(survey_event_return_cols))

  # collect data if required
  if (collect)
    out <- out %>% collect()

  # and return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_site_info <- function(x = NULL, pattern = NULL, collect = FALSE) {

  # grab coords for all sites
  site_info <- fetch_table("site", collect = FALSE) %>%
    dplyr::select(id_site, waterbody, geom_pnt) %>%
    dplyr::mutate(
      longitude = st_x(geom_pnt),
      latitude = st_y(geom_pnt)
    ) %>%
    dplyr::left_join(
      fetch_table("site_project_lu", collect = FALSE) %>%
        dplyr::select(id_site, site_name, id_project),
      by = "id_site"
    ) %>%
    dplyr::arrange(id_site)

  # filter based on columns of x if provided
  if (!is.null(x)) {

    # grab names of all fields in x
    xcol <- colnames(x)

    # and filter on these one at a time
    if ("waterbody" %in% xcol) {
      site_info <- site_info %>%
        dplyr::filter(waterbody %in% !!unique(x$waterbody))
    }
    if ("id_site" %in% xcol) {
      site_info <- site_info %>%
        dplyr::filter(id_site %in% !!unique(x$id_site))
    }
    if ("site_name" %in% xcol) {
      site_info <- site_info %>%
        dplyr::filter(site_name %in% !!unique(x$site_name))
    }
    if ("id_project" %in% xcol) {
      site_info <- site_info %>%
        dplyr::filter(id_project %in% !!unique(x$id_project))
    }

  }

  # filters waterbodies based on regex if provided
  if (!is.null(pattern))
    site_info <- site_info %>% dplyr::filter(grepl(!!pattern, x = waterbody))


  # collect data if required
  if (collect)
    site_info <- site_info %>% collect()

  # return
  site_info

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_species_info <- function(
    x = NULL,
    pattern = NULL,
    taxon = "Fish",
    primary_discipline = "Aquatic fauna",
    collect = FALSE
) {

  taxon_lu <- fetch_table("taxon_lu", collect = FALSE)

  # filter based on columns of x if provided
  if (!is.null(x)) {

    # grab names of all fields in x
    xcol <- colnames(x)

    # and filter on these one at a time
    if ("scientific_name" %in% xcol) {
      taxon_lu <- taxon_lu %>%
        dplyr::filter(scientific_name %in% !!unique(x$scientific_name))
    }
    if ("common_name" %in% xcol) {
      taxon_lu <- taxon_lu %>%
        dplyr::filter(common_name %in% !!unique(x$common_name))
    }
    if ("id_taxon" %in% xcol) {
      taxon_lu <- taxon_lu %>%
        dplyr::filter(id_taxon %in% !!unique(x$id_taxon))
    }

  }

  # filters waterbodies based on regex if provided
  if (!is.null(pattern)) {
    taxon_lu <- taxon_lu %>%
      dplyr::filter(grepl(!!pattern, x = scientific_name))
  }

  # filter to target taxonomic group
  if (!is.null(taxon))
    taxon_lu <- taxon_lu %>% dplyr::filter(taxon_type %in% !!taxon)

  # and discipline
  if (!is.null(primary_discipline)) {
    taxon_lu <- taxon_lu %>%
      dplyr::filter(primary_discipline %in% !!primary_discipline)
  }

  # collect data if required
  if (collect)
    taxon_lu <- taxon_lu %>% collect()

  # return
  taxon_lu

}


# internal function to fetch survey event info
fetch_survey_event <- function(project_id) {

  # grab survey event info
  survey_event_info <- fetch_table("site", collect = FALSE) %>%
    dplyr::select(id_site, waterbody, site_desc) %>%
    dplyr::inner_join(
      fetch_table("survey", collect = FALSE) %>%
        dplyr::select(
          id_site, id_survey, id_project, sdate, gear_type, released
        ),
      by = "id_site"
    ) %>%
    dplyr::mutate(syear = year(sdate)) %>%
    dplyr::rename(
      survey_year = syear,
      survey_date = sdate
    ) %>%
    dplyr::left_join(
      fetch_table("survey_event", collect = FALSE) %>%
        dplyr::select(
          id_survey, id_surveyevent, time_start, time_finish, condition
        ),
      by = "id_survey"
    ) %>%
    dplyr::left_join(
      fetch_table("site_project_lu", collect = FALSE) %>%
        dplyr::select(id_site, id_project, site_name),
      by = c("id_site", "id_project")
    ) %>%
    dplyr::filter(
      id_project %in% project_id,
      released
    ) %>%
    dplyr::mutate(
      id_site = as.integer(id_site),
      id_survey = as.integer(id_survey),
      id_surveyevent = as.integer(id_surveyevent)
    ) %>%
    dplyr::select(-released)

}

# internal function to add electrofishing survey metadata
add_electro <- function(x) {
  x %>%
    dplyr::left_join(
      fetch_table("electro", collect = FALSE) %>%
        dplyr::select(id_surveyevent, seconds),
      by = "id_surveyevent"
    )
}

# internal function to add netting metadata
add_netting <- function(x) {
  x %>%
    dplyr::left_join(
      fetch_table("netting", collect = FALSE) %>%
        dplyr::select(
          id_surveyevent, id_netting, soak_minutes_per_unit, gear_count
        ),
      by = "id_surveyevent"
    )
}

# internal function to fetch taxon lookup table
fetch_taxon_lu <- function(...) {
  fetch_table("taxon_lu", collect = FALSE) %>%
    dplyr::select(id_taxon, scientific_name, common_name)
}

# internal function to fetch info on collected taxa
fetch_collected <- function(survey_event, taxon_lu) {

  # grab collected table from aaedb and add empty fields
  #   for compatibility with observation table

  fetch_table("sample", collect = FALSE) %>%
    dplyr::select(
      id_surveyevent,
      id_sample,
      id_taxon,
      fork_length_cm,
      length_cm,
      weight_g,
      collected
    ) %>%
    dplyr::mutate(
      id_observation = as.integer(NA),
      observed = as.integer(NA),
      id_surveyevent = as.integer(id_surveyevent),
      id_observation = as.integer(id_observation),
      id_sample = as.integer(id_sample)
    ) %>%
    dplyr::inner_join(
      survey_event %>% dplyr::select(id_surveyevent),
      by = "id_surveyevent"
    ) %>%
    dplyr::inner_join(taxon_lu, by = "id_taxon")

}

# internal function to fetch info on observed taxa
fetch_observed <- function(survey_event, taxon_lu) {

  # grab observation table from aaedb and add empty fields
  #   for compatibility with collected table
  fetch_table("observation", collect = FALSE) %>%
    dplyr::select(id_surveyevent, id_observation, id_taxon, count) %>%
    dplyr::rename(observed = count) %>%
    dplyr::mutate(
      id_sample = as.integer(NA),
      fork_length_cm = as.numeric(NA),
      length_cm = as.numeric(NA),
      weight_g = as.numeric(NA),
      collected = as.integer(NA),
      id_surveyevent = as.integer(id_surveyevent),
      id_observation = as.integer(id_observation)
    ) %>%
    dplyr::left_join(
      survey_event %>% dplyr::select(id_surveyevent),
      by = "id_surveyevent"
    ) %>%
    dplyr::inner_join(taxon_lu, by = "id_taxon") %>%
    dplyr::select(dplyr::all_of(colnames(taxa_collected)))

}

# internal list of project names by id
source_project_name <- c(
  "Snags",
  "VEFMAP",
  NA,
  "NFRC",
  NA,
  "Kiewa Ovens",
  "SRA",
  "Southern Basins",
  "Ovens Demo Reaches",
  "King Parrot Creek Macquarie Perch",
  "Lower Goulburn Projects",
  "Hughes Creek Macquarie Perch",
  "Seven Creeks Macquarie Perch",
  "Index of Estuarine Condition",
  "LTIM Lower Goulburn",
  "IVT Broken Creek"
)

# internal list of variables to be returned from database
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
  "id_taxon",
  "scientific_name",
  "common_name",
  "fork_length_cm",
  "length_cm",
  "weight_g",
  "collected",
  "observed",
  "extracted_ts"
)
