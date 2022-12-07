# functions to ensure connection is closed when the package is
#   unloaded or an R session is ended
# From: https://www.r-bloggers.com/2022/03/closing-database-connections-in-r-packages/
closeConnection <- function(e, conn_name = "conn") {
  # Want to be as defensive as possible, so if there is no connection, we don't want to test it
  if (conn_name %in% ls(e)) {
    conn <- get(conn_name, envir = e)
    # If connection has closed for any other reason, we don't want the function to error
    if (DBI::dbIsValid(conn)) {
      DBI::dbDisconnect(conn)
    }
  }
}
.onLoad <- function(libname, pkgname) {
  reg.finalizer(
    e = DB_ENV,
    f = closeConnection,
    onexit = TRUE
  )
}
.onUnload <- function(libpath) {
  closeConnection(DB_ENV)
}
