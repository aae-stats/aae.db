#' @name fetch_data
#'
#' @title Query the AAEDB and return a query object to be manipulated
#'   and downloaded
#'
#' @export
#'
#' @importFrom rstudioapi askForPassword
#' @importFrom dplyr tbl
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
#' @param range a vector containing minimum and maximum years for
#'   which data are required
#'@param collect logical: should a query be executed and the full
#'   data set returned (default, \code{TRUE}) or should the query
#'   be returned for further processing prior to execution? See description
#' @param \dots additional arguments passed to \link[dbplyr]{tbl_sql}
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
#' vefmap <- fetch_table("v_vefmap_only_flat_data", collect = FALSE)
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
#' site_data <- fetch_table("site", collect = FALSE)
#' survey_data <- fetch_table("survey", collect = FALSE)
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
#' ovens_data <- fetch_project(9, collect = FALSE)
#' ovens_data <- ovens_data %>% collect()
#'
#' # repeat this for a subset of years
#' ovens_data <- fetch_project(9, range = c(2015, 2017), collect = FALSE)
#' ovens_data <- ovens_data %>% collect()
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' #   when all queries and evaluation is complete
#' # aaedb_disconnect()
#'
#' @rdname fetch_data
fetch_table <- function(x, schema = "aquatic_data", collect = TRUE, ...) {

  # connect to database if required
  if (!check_aaedb_connection()) {

    # and kick this connection if the query is executed
    if (collect)
      on.exit(aaedb_disconnect())

    # connect to aaedb
    aaedb_connect()

    # print a note that a database connection is open if
    #   not collecting
    if (!collect) {
      cat(
        "fetch_table has opened a connection to the AAEDB. ",
        "This connection must remain open while working with ",
        "the returned query but can be closed with aaedb_disconnect() ",
        "once the query has been executed."
      )
    }

  }

  # print a note that automatic execution of the query will stop in
  #   the next version
  if (collect) {
    cat(
      "collect is TRUE (the default), which means the query will be executed",
      "and the full data set returned. This default setting will reverse",
      "in version 0.1.0 of the aae.db package and returned queries will need",
      "to be executed with the collect() function. To return unevaluated",
      "queries in the current version of aaa.db, set collect = FALSE."
    )
  }

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
fetch_query <- function(query, collect = TRUE, ...) {

  # query must be a function or string
  if (!is.character(query))
    stop("query must be a SQL query (string)", call. = FALSE)

  # check that the query isn't a vector
  if (length(query) > 1)
    stop("SQL query has length > 1 but must be a string", .call = FALSE)

  # connect to database if required
  if (!check_aaedb_connection()) {

    # and kick this connection if the query is executed
    if (collect)
      on.exit(aaedb_disconnect())

    # connect to aaedb
    aaedb_connect()

    # print a note that a database connection is open if
    #   not collecting
    if (!collect) {
      cat(
        "fetch_query has opened a connection to the AAEDB. ",
        "This connection must remain open while working with ",
        "the returned query but can be closed with aaedb_disconnect() ",
        "once the query has been executed."
      )
    }

  }

  # print a note that automatic execution of the query will stop in
  #   the next version
  if (collect) {
    cat(
      "collect is TRUE (the default), which means the query will be executed",
      "and the full data set returned. This default setting will reverse",
      "in version 0.1.0 of the aae.db package and returned queries will need",
      "to be executed with the collect() function. To return unevaluated",
      "queries in the current version of aaa.db, set collect = FALSE."
    )
  }

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
fetch_project <- function(
    project_id,
    range = c(1900, 2099),
    schema = "aquatic_data",
    collect = TRUE,
    ...
) {

  # query must be a function or string
  if (!project_id %in% c(1, 2, 4, 6:16)) {
    stop(
      "project_id must be a valid integer (see ?fetch_project for details)",
      call. = FALSE
    )
  }

  # check that the query isn't a vector if it's a string
  if (length(range) != 2) {
    stop(
      "range must be a vector with two values (min. and max. year)",
      .call = FALSE
    )
  }

  # connect to database if required
  if (!check_aaedb_connection()) {

    # and kick this connection if the query is executed
    if (collect)
      on.exit(aaedb_disconnect())

    # connect to aaedb
    aaedb_connect()

    # print a note that a database connection is open if
    #   not collecting
    if (!collect) {
      cat(
        "fetch_project has opened a connection to the AAEDB.",
        "This connection must remain open while working with",
        "the returned query but can be closed with aaedb_disconnect()",
        "once the query has been executed."
      )
    }

  }

  # print a note that automatic execution of the query will stop in
  #   the next version
  if (collect) {
    cat(
      "collect is TRUE (the default), which means the query will be executed",
      "and the full data set returned. This default setting will reverse",
      "in version 0.1.0 of the aae.db package and returned queries will need",
      "to be executed with the collect() function. To return unevaluated",
      "queries in the current version of aaa.db, set collect = FALSE."
    )
  }

  # define the query for a given project and range
  query_string <- paste0(
    "get_project_data(", project_id, ", ", range[1], ", ", range[2], ")"
  )

  # download data for a single project using the get_project_data() function
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(query_string), ...)
  )

  # collect data if required
  if (collect)
    out <- out %>% collect()

  # and return
  out

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
