#' @name list_tables
#'
#' @title List available tables in the AAEDB
#'
#' @export
#'
#' @importFrom dplyr arrange filter select
#' @importFrom rlang `!!`
#'
#' @param schema the schema from which to list tables, defaults to
#'   \code{"aquatic_data"} and can be one of \code{"aquatic_data"},
#'   \code{"public"}, \code{"spatial"}, \code{"spatial_isc"},
#'   or \code{"stream_network"},
#' @param \dots additional arguments passed to \link[dbplyr]{tbl_sql}
#' @param collect logical: should a query be executed (\code{TRUE}) or
#'   evaluated lazily (\code{FALSE}, the default)
#'
#' @description \code{list_tables} returns a list of all tables in a
#'   specified schema in the AAEDB. This information can be used to
#'   view and generate queries based on all tables in the database. See
#'   \code{fetch_data} for examples of generating queries.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # dplyr methods used below
#' library(dplyr)
#'
#' # list all tables in the aquatic_data schema
#' list_tables() %>% collect()
#'
#' # list all tables in the spatial schema
#' list_tables(schema = "spatial") %>% collect()
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' #   when all queries and evaluation is complete
#' # aaedb_disconnect()
#'
#' @rdname list_tables
list_tables <- function(schema = "aquatic_data", ..., collect = FALSE) {

  # check that schema is in the viewable list
  if (!schema %in% available_schema) {
    stop(
      "schema must be one of ",
      paste(available_schema[-length(available_schema)], collapse = ", "),
      ", or ", available_schema[length(available_schema)],
      call. = FALSE
    )
  }


  # list all the tables in the requested schema, sorted by table_type
  table <- fetch_table("tables", schema = "information_schema", ...) %>%
    filter(table_schema == !!schema) %>%
    select(table_catalog, table_schema, table_name, table_type) %>%
    arrange(table_type)

  # collect if required
  if (collect)
    table <- table %>% collect()

  # return
  table

}

# list of available schema (excluding sandboxes)
available_schema <- c(
  "aquatic_data",
  "public",
  "spatial",
  "spatial_isc",
  "stream_network"
)
