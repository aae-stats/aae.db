# internal function: set aaedb_promise class
as_aaedb_promise <- function(x) {
  as_class(x, name = "aaedb_promise", type = "tbl_PqConnection")
}

# set an object class
as_class <- function(
    object,
    name,
    type = "tbl_PqConnection"
) {

  type <- match.arg(type)
  stopifnot(inherits(object, type))

  class(object) <- c(name, class(object))

  object

}
