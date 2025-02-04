#' @name aaedb_connect
#'
#' @title Open a connection to the AAEDB
#'
#' @export
#'
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
#'   \code{aaedb_connect} will ask for a username and password each time
#'   it is called. To save this information locally, the
#'   \code{aaedb_key_set} function allows a user to store their details
#'   in the system credential store (e.g. keychain). On first call,
#'   \code{aaedb_key_set} will ask for a username and password, after
#'   which this information will be stored and will persist once the
#'   R session is closed. The \code{aaedb_key_delete} function can be used
#'   to remove this saved key.
#'
#'   An open connection will automatically be detected by the
#'   \code{fetch_table}, \code{fetch_query}, and \code{fetch_project}
#'   functions.
#'
#'   It is good practice to close the database connection when all queries
#'   are completed. This will happen automatically when the current
#'   R session ends. If the connection needs to be closed prior to this,
#'   it can be manually closed by calling \code{aaedb_disconnect()}.
#'
#' @examples
#' # optional: store credentials to avoid re-entering every time
#' aaedb_key_set()
#'
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # process a basic query on the database (list all site information
#' #   for the Ovens River)
#' site_table <- fetch_table("site", collect = FALSE)
#' ovens_sites <- site_table %>%
#'   filter(grepl("ovens", waterbody, ignore.case = TRUE)) %>%
#'   collect()
#'
#' # manually disconnect from the AAEDB (not usually required)
#' aaedb_disconnect()
aaedb_connect <- function(...) {

  # adapted from: https://www.r-bloggers.com/2022/03/closing-database-connections-in-r-packages/

  # check whether credentials are stored locally,
  #   if not, use rstudioapi to get creds
  pwd_method <- "keyring"
  if (nrow(key_list("aae_db")) == 0)
    pwd_method <- "rstudioapi"

  # open connection if none exists already
  if (is.null(DB_ENV$conn)) {

    DB_ENV$conn <- DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = "aae_db",
      host = "dev-aae-psql-01.postgres.database.azure.com",
      port = "5432",
      user = get_credentials(pwd_method, type = "username"),
      password = get_credentials(pwd_method, type = "password")
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
      host = "dev-aae-psql-01.postgres.database.azure.com",
      port = "5432",
      user = get_credentials(pwd_method, type = "username"),
      password = get_credentials(pwd_method, type = "password")
    )

  }

  # return TRUE if successful
  invisible(TRUE)

}

#' @title Store database credentials locally
#'
#' @export
#'
#' @importFrom keyring key_set
#'
#' @rdname aaedb_connect
aaedb_key_set <- function(...) {
  uid <- rstudioapi::askForPassword("Database username")
  keyring::key_set(service = "aae_db", username = uid)
}

#' @title Store database credentials locally
#'
#' @export
#'
#' @importFrom keyring key_delete
#'
#' @rdname aaedb_connect
aaedb_key_delete <- function(...) {
  uid <- rstudioapi::askForPassword("Database username")
  keyring::key_delete(service = "aae_db", username = uid)
}

#' @title Close a connection to the AAEDB
#'
#' @export
#'
#' @rdname aaedb_connect
aaedb_disconnect <- function(...) {
  closeConnection(DB_ENV)
}

# internal function to ask for a password or get stored
#   credentials
#' @importFrom rstudioapi askForPassword
#' @importFrom keyring key_list key_get
get_credentials <- function(method, type) {

  # check method is supported
  if (!method %in% c("keyring", "rstudioapi"))
    stop("method must be one of `keyring` or `rstudioapi`", call. = FALSE)

  # check that type is supported
  if (!type %in% c("username", "password"))
    stop("type must be one of `username` or `password`", call. = FALSE)

  # use rstudioapi if creds aren't stored locally
  if (method == "rstudioapi") {

    # ask for info with appropriate phrase based on type
    id <- rstudioapi::askForPassword(paste0("Database ", type))

  } else {

    # otherwise can use stored credentials for user or pwd
    id <- key_list("aae_db")[1, 2]
    if (type == "password")
      id <- key_get("aae_db", id)

  }

  # return
  id

}

#' @title Store database credentials locally
#'
#' @export
#'
#' @importFrom DBI dbIsValid
#'
#' @rdname aaedb_connect
check_aaedb_connection <- function(...) {
  valid <- !is.null(DB_ENV$conn)
  if (valid)
    valid <- DBI::dbIsValid(DB_ENV$conn)
  valid
}

# internal function to connect to the DB if not already connected
connect_if_required <- function(.call, collect) {

  # default to using open connection
  new_connection <- FALSE

  # check connection and open a new connection if required
  if (!check_aaedb_connection()) {

    # connect to aaedb
    aaedb_connect()

    # print a note that a database connection is open if
    #   not collecting
    if (!collect) {
      cat(
        .call,
        "has opened a connection to the AAEDB.",
        "This connection must remain open while working with",
        "the returned query but can be closed with aaedb_disconnect()",
        "once the query has been executed.\n\n"
      )
    }

    # update flag to note new connection
    new_connection <- TRUE

  }

  # return
  new_connection

}

# set up a new environment for the database connection
DB_ENV <- new.env()
