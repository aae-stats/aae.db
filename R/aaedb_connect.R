#' @name aaedb_connect
#'
#' @title Open a connection to the AAEDB
#'
#' @export
#'
#' @importFrom rstudioapi askForPassword
#' @importFrom DBI dbConnect dbDisconnect dbIsValid
#' @importFrom RPostgres Postgres
#'
#' @param \dots ignored
#'
#' @description \code{aaedb_connect} opens a connection to the AAEDB,
#'   which can be useful when making repeated queries to the AAEDB as
#'   it avoids repeatedly entering user credentials. Connections to the
#'   AAEDB require an appropriate VPN connection.
#'
#'   An open connection will be automatically detected by the
#'   \code{fetch_table} and \code{fetch_query} functions.
#'
#'   It is good practice to close the database connection when all queries
#'   are completed. This will happen automatically when the current
#'   R session ends. If the connection needs to be closed prior to this,
#'   it can be manually closed by calling \code{aaedb_disconnect()}.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # process a basic query on the database
#' query_fn <- function(x) {
#'   tbl(x, in_schema(sql("aquatic_data"), sql("site"))) %>%
#'     filter(waterbody == "Ovens River")
#' }
#' ovens_sites <- fetch_query(query_fn)
#'
#' # manually disconnect from the AAEDB (not usually required)
#' aaedb_disconnect()
aaedb_connect <- function(...) {

  # taken from: https://www.r-bloggers.com/2022/03/closing-database-connections-in-r-packages/

  # open connection if none exists already
  if (is.null(DB_ENV$conn)) {

    DB_ENV$conn <- DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = "aae_db",
      host = "10.110.7.134",
      port = "5432",
      user = rstudioapi::askForPassword("Database username"),
      password = rstudioapi::askForPassword("Database password")
    )

  } else {

    # otherwise, close the existing connection
    if (DBI::dbIsValid(DB_ENV$conn)) {
      DBI::dbDisconnect(DB_ENV$conn)
    }

    # and create a new connection
    DB_ENV$conn <- DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = "aae_db",
      host = "10.110.7.134",
      port = "5432",
      user = rstudioapi::askForPassword("Database username"),
      password = rstudioapi::askForPassword("Database password")
    )

  }

  # return TRUE if successful
  invisible(TRUE)

}

#' @title Close a connection to the AAEDB
#'
#' @export
#'
#' @rdname aaedb_connect
aaedb_disconnect <- function(...) {
  closeConnection(DB_ENV)
}

# internal function to check if a connection exists
check_aaedb_connection <- function() {
  valid <- !is.null(DB_ENV$conn)
  if (valid)
    valid <- DBI::dbIsValid(DB_ENV$conn)
  valid
}

# set up a new environment for the database connection
DB_ENV <- new.env()
