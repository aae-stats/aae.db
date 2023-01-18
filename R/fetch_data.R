#' @name fetch_data
#'
#' @title Query the AAEDB
#'
#' @export
#'
#' @importFrom rstudioapi askForPassword
#' @importFrom dplyr tbl collect mutate
#' @importFrom dbplyr sql in_schema
#'
#' @param x a character specifying an individual table in the AAEDB
#' @param schema schema in which \code{x} is found. Defaults to
#'   \code{"aquatic_data"}
#' @param query a character specifying a SQL query or a function
#'   that takes a single argument (the database connection) and
#'   processes a dplyr-style series of operations
#' @param project_id an integer specifying an individual AAE project
#'   (1 - Snags, 2 - VEFMAP, 4 - NFRC, 6 - Kiewa Ovens, 7 - SRA,
#'    8 - Southern Basins, 9 - Ovens Demo Reaches, 10 - King Parrot
#'    Creek Macquarie Perch, 11 - Lower Goulburn Projects, 12 - Hughes
#'    Creek Macquarie Perch, 13 - Seven Creeks Macquarie Perch,
#'    14 - Index of Estuarine Condition, 15 - LTIM Lower Goulburn,
#'    16 - IVT Broken Creek)
#' @param range a vector containing minimum and maximum years for
#'   which data are required
#' @param \dots additional arguments passed to function-valued
#'   \code{query} arguments in \code{fetch_query} (ignored otherwise)
#'
#' @description \code{fetch_table}, \code{fetch_query}, and
#'   \code{fetch_project} represent three ways to interact with the
#'   AAEDB. \code{fetch_table} provides access to prepared tables in
#'   the database, \code{fetch_query} allows users to compute custom
#'   queries, and \code{fetch_project} prepares and downloads data
#'   for an individual AAE project.
#'
#'   All functions require credentials to access the AAEDB, as well
#'   as a appropriate VPN connection. If making multiple queries,
#'   it can be easier to connect once to the AAEDB rather than repeatedly
#'   connecting (and disconnecting). This is possible with the
#'   \code{aaedb_connect} function.
#'
#'   \code{fetch_query} can be used to download anything
#'   you would download with \code{fetch_table} or \code{fetch_project}.
#'   The benefits of \code{fetch_table} and \code{fetch_project} are in
#'   providing access to prepared tables containing commonly used variables
#'   for analyses of fish data. The benefit of \code{fetch_query} is in
#'   allowing custom queries and allowing processing of data files to occur
#'   on the server prior to downloading the final data set. This may be
#'   especially useful when working with large spatial data sets.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # download the VEFMAP database flat file
#' vefmap <- fetch_table("v_vefmap_only_flat_data")
#'
#' # process a simple SQL query to list all projects with data from the
#' #   Ovens river
#' survey_info <- fetch_query(
#'   "SELECT waterbody, id_project
#'      FROM aquatic_data.site a LEFT JOIN aquatic_data.survey b
#'      ON a.id_site = b.id_site
#'      WHERE lower(waterbody) LIKE 'ovens%'
#'      GROUP BY waterbody, id_project
#'      ORDER by waterbody, id_project"
#' )
#'
#' # process a dplyr query to collect site information
#' query_fn <- function(x) {
#'   tbl(x, in_schema(sql("aquatic_data"), sql("site"))) %>%
#'     filter(waterbody == "Ovens River")
#' }
#' ovens_sites <- fetch_query(query_fn)
#'
#' # and grab information for individual sites
#' ovens_data <- fetch_project(9)
#'
#' # possibly for a subset of years
#' ovens_data <- fetch_project(9, range = c(2015, 2017))
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' # aaedb_disconnect()
#'
#' @rdname fetch_data
fetch_table <- function(x, schema = "aquatic_data", ...) {

  # connect to database if required (but disconnect on exit)
  if (!check_aaedb_connection()) {

    # make sure to disconnect from db on exit
    on.exit(aaedb_disconnect())

    # connect to db
    aaedb_connect()

  }

  # view flat file from specified schema
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(x))
  )

  # and collect
  out <- dplyr::collect(out)

  # return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_query <- function(query, ...) {

  # query must be a function or string
  if (!is.character(query) & !is.function(query)) {
    stop(
      "query must be a SQL query (string) or function specifying ",
      "a series of dplyr-stlye operations",
      call. = FALSE
    )
  }

  # check that the query isn't a vector if it's a string
  if (is.character(query) & length(query) > 1) {
    stop(
      "SQL query has length > 1 but must be a single string",
      .call = FALSE
    )
  }

  # connect to database if required (but disconnect on exit)
  if (!check_aaedb_connection()) {

    # make sure to disconnect from db on exit
    on.exit(aaedb_disconnect())

    # connect to db
    aaedb_connect()

  }

  # grab query, assuming it's either a string SQL query or
  #   a function specifying a set of dbplyr actions
  if (is.character(query)) {
    out <- dplyr::tbl(DB_ENV$conn, dbplyr::sql(query))
  } else {
    out <- query(DB_ENV$conn, ...)
  }

  # and collect
  out <- dplyr::collect(out)

  # return
  out

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_project <- function(
    project_id, range = c(1900, 2099), schema = "aquatic_data", ...
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

  # connect to database if required (but disconnect on exit)
  if (!check_aaedb_connection()) {

    # make sure to disconnect from db on exit
    on.exit(aaedb_disconnect())

    # connect to db
    aaedb_connect()

  }

  # define the query for a given project and range
  query_string <- paste0(
    "get_project_data(", project_id, ", ", range[1], ", ", range[2], ")"
  )

  # download data for a single project using the get_project_data() function
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(query_string))
  )

  # and collect
  out <- dplyr::collect(out)

  # add a project name column to the output
  out <- mutate(out, source_project_name = aae.db:::source_project_name[project_id])

  # return
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
