#' @name fetch_data
#'
#' @title Query the AAEDB
#'
#' @export
#'
#' @import RPostgres DBI dplyr
#' @importFrom dbplyr in_schema
#'
#' @param x a character specifying an individual table in the AAEDB
#' @param schema schema in which \code{x} is found. Defaults to
#'   \code{"aquatic_data"}
#'
#' @description \code{fetch_table} and \code{fetch_query} represent two
#'   ways to interact with the AAEDB. \code{fetch_table} provides access
#'   to existing flat tables in the database, whereas \code{fetch_query}
#'   allows users to compute custom queries.
#'
#'   Both functions require credentials to access the AAEDB, plus appropriate
#'   access and credentials to the relevant VPN.
#'
#'   \code{fetch_query} is NOT CURRENTLY IMPLEMENTED.
#'
#' @examples
#' # to add
#'
#'

#' @rdname fetch_data
fetch_table <- function(x, schema = "aquatic_data") {

  # make sure to disconnect from db on exit
  on.exit(dbDisconnect(con))

  # connect to db
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = "arispatialdb",
    host = "ari-spatial-poc-db.cluster-custom-cepp1cnsvaah.ap-southeast-2.rds.amazonaws.com",
    port = "5432",
    user = rstudioapi::askForPassword("Database username"),
    password = rstudioapi::askForPassword("Database password")
  )

  # view flat file from specified schema
  out <- tbl(con, in_schema(sql(schema), sql(x)))

  # and collect
  out <- out %>% collect()

  # return
  out

}

#' @rdname fetch_data
#'
fetch_query <- function(query) {

  # make sure to disconnect from db on exit
  on.exit(dbDisconnect(con))

  # connect to db
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = "arispatialdb",
    host = "ari-spatial-poc-db.cluster-custom-cepp1cnsvaah.ap-southeast-2.rds.amazonaws.com",
    port = "5432",
    user = rstudioapi::askForPassword("Database username"),
    password = rstudioapi::askForPassword("Database password")
  )

  ## USE sql() for generic queries
  ## TODO: see if there's a way to pass a dbplyr query. Maybe as a fn?
  # if (is.character(query)) {
  #   # use sql(query)
  # } else {
  # #query(con)
  # }

  # apply specified query to con
  out <- NULL

  # return
  out

}
