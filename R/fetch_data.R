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
#' @param \dots additional arguments passed to \code{print};
#'   ignored otherwise
#'  @param n argument passed to \link[dplyr]{print} (defaults to NULL)
#'  @param width argument passed to \link[dplyr]{print} (defaults to NULL)
#'  @param n_extra argument passed to \link[dplyr]{print} (defaults to NULL)
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
#'   Update: the \code{fetch_} functions now return a database connection
#'   rather than a downloaded table. This allows further filtering or
#'   changes to the query using \code{dplyr} methods prior to downloading
#'   the data. This is especially useful when downloading a subset of a
#'   much larger data sets.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # download the VEFMAP database flat file
#' vefmap <- fetch_table("v_vefmap_only_flat_data")
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
#'      ORDER by waterbody, id_project"
#' )
#' survey_info <- survey_info %>% collect()
#'
#' # process a dplyr query to reduce the data set prior
#' #   to downloading
#' ovens_sites <- fetch_table("site")
#' ovens_sites <- ovens_sites %>%
#'   filter(waterbody == "Ovens River") %>%
#'   collect()
#'
#' # and grab information for individual sites
#' ovens_data <- fetch_project(9)
#' ovens_data <- ovens_data %>% collect()
#'
#' # possibly for a subset of years
#' ovens_data <- fetch_project(9, range = c(2015, 2017))
#' ovens_data <- ovens_data %>% collect()
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' # aaedb_disconnect()
#'
#' @rdname fetch_data
fetch_table <- function(x, schema = "aquatic_data", ...) {

  # connect to database if required
  if (!check_aaedb_connection())
    aaedb_connect()

  # view flat file from specified schema
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(x))
  )

  # return
  as_aaedb_promise(out)

}

#' @rdname fetch_data
#'
#' @export
#'
fetch_query <- function(query, ...) {

  # query must be a function or string
  if (!is.character(query))
    stop("query must be a SQL query (string)", call. = FALSE)

  # check that the query isn't a vector
  if (length(query) > 1)
    stop("SQL query has length > 1 but must be a string", .call = FALSE)

  # connect to database if required
  if (!check_aaedb_connection())
    aaedb_connect()

  # grab query, assuming it's a string SQL query
  out <- dplyr::tbl(DB_ENV$conn, dbplyr::sql(query))

  # return
  as_aaedb_promise(out)

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

  # connect to database if required
  if (!check_aaedb_connection())
    aaedb_connect()

  # define the query for a given project and range
  query_string <- paste0(
    "get_project_data(", project_id, ", ", range[1], ", ", range[2], ")"
  )

  # download data for a single project using the get_project_data() function
  out <- dplyr::tbl(
    DB_ENV$conn,
    dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(query_string))
  )

  # and return
  as_aaedb_promise(out)

}

#' @rdname fetch_data
#'
#' @export
print.aaedb_promise <- function (
    x, ..., n = NULL, width = NULL, n_extra = NULL
) {
  dbplyr:::print.tbl_sql(x, ..., n = NULL, width = NULL, n_extra = NULL)
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
