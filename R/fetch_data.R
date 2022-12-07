#' @name fetch_data
#'
#' @title Query the AAEDB
#'
#' @export
#'
#' @importFrom rstudioapi askForPassword
#' @importFrom dplyr tbl collect
#' @importFrom dbplyr sql in_schema
#'
#' @param x a character specifying an individual table in the AAEDB
#' @param schema schema in which \code{x} is found. Defaults to
#'   \code{"aquatic_data"}
#' @param query a character specifying a SQL query or a function
#'   that takes a single argument (the database connection) and
#'   processes a dplyr-style series of operations
#' @param \dots ignored
#'
#' @description \code{fetch_table} and \code{fetch_query} represent two
#'   ways to interact with the AAEDB. \code{fetch_table} provides access
#'   to single tables in the database, whereas \code{fetch_query}
#'   allows users to compute custom queries.
#'
#'   Both functions require credentials to access the AAEDB, as well
#'   as a appropriate VPN connection.
#'
#'   In most cases, \code{fetch_table} can be used to download anything
#'   you would download with \code{fetch_query}. However, \code{fetch_table}
#'   will collect the full table each time it is processed, which will
#'   then require post-processing on your local system. \code{fetch_query}
#'   has not been extensively tested but provides an alternative and will
#'   process data files on the server prior to downloading the final product.
#'
#'   If making multiple queries, it can be easier to connect once to the
#'   AAEDB rather than repeatedly connecting (and disconnecting). This
#'   is possible with the \code{aaedb_connect} function.
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
  out <- dplyr::tbl(DB_ENV$conn, dbplyr::in_schema(dbplyr::sql(schema), dbplyr::sql(x)))

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
    out <- query(DB_ENV$conn)
  }

  # and collect
  out <- dplyr::collect(out)

  # return
  out

}
