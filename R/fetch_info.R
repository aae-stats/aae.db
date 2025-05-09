#' @name fetch_info
#'
#' @title Extract information on sites or species represented in the AAEDB
#'
#' @export
#'
#' @importFrom methods hasArg
#' @importFrom dplyr across all_of arrange filter if_any left_join mutate
#'   select
#' @importFrom rlang `!!` parse_expr
#'
#' @param x an evaluated or unevaluated query (table) for which to extract
#'   site or species information with \code{fetch_site_info} and
#'   \code{fetch_species_info}
#' @param \dots additional arguments used to filter outputs
#'   based on regex expressions (see \link[base]{grepl} for possible
#'   arguments)
#' @param collect logical: should a query be executed (\code{TRUE}) or
#'   evaluated lazily (\code{FALSE}, the default)
#'
#' @description \code{fetch_site_info}, \code{fetch_survey_info}, and
#'   \code{fetch_species_info} return details of sites, surveys, or species
#'   included in the AAEDB. This information can
#'   be filtered to sites or species represented in a query or data set or
#'   based on regex expressions. See examples for usage.
#'
#' @examples
#' # connect to the AAEDB
#' aaedb_connect()
#'
#' # dplyr methods used below
#' library(dplyr)
#'
#' # set up a query that includes the full VEFMAP data set
#' vefmap <- fetch_project(2)
#'
#' # can manipulate and filter this query with dplyr methods
#' vefmap <- vefmap |>
#'   filter(
#'     waterbody == "Campaspe River",
#'     scientific_name == "Maccullochella peelii"
#'   )
#'
#' # evaluate this query with collect
#' vefmap <- vefmap |> collect()
#'
#' # fetch information on the sites in a data set
#' vefmap_site_info <- fetch_site_info(vefmap)
#'
#' # "spatialise" this information with the `sf` package
#' library(sf)
#' vefmap_site_info <- vefmap_site_info |>
#'   filter(!is.na(geom_pnt)) |>
#'   collect()
#' vefmap_sf <- vefmap_site_info |>
#'   st_set_geometry(st_as_sfc(vefmap_site_info$geom_pnt))
#'
#' # and make a basic map with the `mapview` package
#' library(mapview)
#' vefmap_sf |>
#'   select(-geom_pnt) |>
#'   mapview(
#'     col.regions = "DarkGreen",
#'     label = "site_name",
#'     layer.name = "Survey site"
#'   )
#'
#' # alternatively, extract information on all sites where any field of the
#' #   site table matches a regex pattern
#' murray_site_info <- fetch_site_info(pattern = "^Murray", ignore.case = TRUE)
#'
#' # fetch information on the surveys in a data set
#' vefmap_survey_info <- fetch_survey_info(vefmap)
#'
#' # extract information on the species in a data set
#' fetch_species_info(vefmap)
#'
#' # or for all species with scientific names matching a regex pattern
#' fetch_species_info(pattern = "Maccull", ignore.case = TRUE)
#'
#' # list all taxonomic groups for use in fetch_species_info
#' fetch_table("taxon_lu", collect = FALSE) |>
#'   select(taxon_type) |>
#'   collect() |>
#'   pull(taxon_type) |>
#'   unique()
#'
#' # or list all primary disciplines for use in fetch_species_info
#' fetch_table("taxon_lu", collect = FALSE) |>
#'   select(primary_discipline) |>
#'   collect() |>
#'   pull(primary_discipline) |>
#'   unique()
#'
#' # and download data for one group (setting primary_discipline to NULL
#' #   to override default setting)
#' fetch_species_info(pattern = "Aquatic invertebrates", ignore.case = TRUE)
#'
#' # optional: disconnect from the AAEDB prior to ending the R session
#' #   when all queries and evaluation is complete
#' # aaedb_disconnect()
#'
#' @rdname fetch_info
fetch_site_info <- function(x = NULL, ..., collect = FALSE) {

  # check x is OK for use
  x_ok <- is.null(x) |
    inherits(x, "tbl_PqConnection") |
    inherits(x, "data.frame")
  if (!x_ok)
    stop("x must be an unevaluated query or tbl/data.frame", call. = FALSE)

  # grab coords for all sites
  site_info <- fetch_table("site") |>
    dplyr::select(id_site, waterbody, geom_pnt) |>
    dplyr::mutate(
      longitude = st_x(geom_pnt),
      latitude = st_y(geom_pnt)
    ) |>
    dplyr::left_join(
      fetch_table("site_project_lu") |>
        dplyr::select(id_site, site_name, id_project),
      by = "id_site"
    ) |>
    dplyr::left_join(fetch_table("reach_lu"), by = "id_site")

  # filter based on columns of x if provided
  if (!is.null(x)) {

    # grab names of all fields in x
    xcol <- colnames(x)

    # pull out main columns of x and collect if it's an unevaluated query
    target_cols <- c(
      "waterbody", "reach_no", "id_site", "site_name", "id_project"
    )
    available_targets <- xcol[xcol %in% target_cols]
    xval <- x |> dplyr::select(dplyr::all_of(available_targets))
    if ("tbl_PqConnection" %in% class(x))
      xval <- xval |> collect()

    # and filter on these one at a time
    for (i in seq_along(available_targets)) {
      unique_xval <- xval |>
        dplyr::select(dplyr::all_of(available_targets[i])) |>
        unlist() |>
        unique()
      site_info <- site_info |>
        dplyr::filter(
          !!rlang::parse_expr(available_targets[i]) %in% !!unique_xval
        )
    }

  }

  # filters fields based on regex if provided
  if (methods::hasArg("pattern")) {

    # set defaults for grepl function and update with arg_list values
    arg_list <- list(...)
    args <- list(
      pattern = arg_list$pattern,
      ignore.case = FALSE,
      perl = FALSE,
      fixed = FALSE,
      useBytes = FALSE
    )
    args[names(arg_list)] <- arg_list

    # filter all string columns based on these settings
    string_cols <- c("waterbody", "site_name")
    for (i in seq_along(string_cols)) {
      site_info <- site_info |>
        dplyr::filter(
          grepl(
            x = !!rlang::parse_expr(string_cols[i]),
            pattern = !!args$pattern,
            ignore.case = !!args$ignore.case,
            perl = !!args$perl,
            fixed = !!args$fixed,
            useBytes = !!args$useBytes
          )
        )
    }

  }

  # collect data if required
  if (collect)
    site_info <- site_info |> collect()

  # and arrange by site
  site_info <- site_info |>
    dplyr::select(
      id_project,
      waterbody,
      reach_no,
      id_site,
      site_name,
      geom_pnt,
      latitude,
      longitude
    )

  # return
  site_info

}

#' @rdname fetch_info
#'
#' @export
#'
fetch_survey_info <- function(x = NULL, ..., collect = FALSE) {

  # check x is OK for use
  x_ok <- is.null(x) |
    inherits(x, "tbl_PqConnection") |
    inherits(x, "data.frame")
  if (!x_ok)
    stop("x must be an unevaluated query or tbl/data.frame", call. = FALSE)

  # grab coords for all sites
  site_info <- fetch_table("site") |>
    dplyr::select(waterbody, id_site, site_name, geom_pnt) |>
    dplyr::mutate(
      longitude = st_x(geom_pnt),
      latitude = st_y(geom_pnt)
    ) |>
    dplyr::left_join(fetch_table("reach_lu"), by = "id_site")
  survey_info <- fetch_survey_table(seq_len(20)) |>
    dplyr::left_join(site_info, by = "id_site")

  # filter based on columns of x if provided
  if (!is.null(x)) {

    # grab names of all fields in x
    xcol <- colnames(x)

    # pull out main columns of x and collect if it's an unevaluated query
    target_cols <- c("waterbody", "reach_no", "id_site", "id_project", "id_survey")
    available_targets <- xcol[xcol %in% target_cols]
    xval <- x |> dplyr::select(dplyr::all_of(available_targets))
    if ("tbl_PqConnection" %in% class(x))
      xval <- xval |> collect()

    # and filter on these one at a time
    for (i in seq_along(available_targets)) {
      unique_xval <- xval |>
        dplyr::select(dplyr::all_of(available_targets[i])) |>
        unlist() |>
        unique()
      survey_info <- survey_info |>
        dplyr::filter(
          !!rlang::parse_expr(available_targets[i]) %in% !!unique_xval
        )
    }

  }

  # filters fields based on regex if provided
  if (methods::hasArg("pattern")) {

    # set defaults for grepl function and update with arg_list values
    arg_list <- list(...)
    args <- list(
      pattern = arg_list$pattern,
      ignore.case = FALSE,
      perl = FALSE,
      fixed = FALSE,
      useBytes = FALSE
    )
    args[names(arg_list)] <- arg_list

    # filter all string columns based on these settings
    string_cols <- c("waterbody", "site_name", "gear_type")
    for (i in seq_along(string_cols)) {
      survey_info <- survey_info |>
        dplyr::filter(
          grepl(
            x = !!rlang::parse_expr(string_cols[i]),
            pattern = !!args$pattern,
            ignore.case = !!args$ignore.case,
            perl = !!args$perl,
            fixed = !!args$fixed,
            useBytes = !!args$useBytes
          )
        )
    }

  }

  # collect data if required
  if (collect)
    survey_info <- survey_info |> collect()

  # and arrange by site
  survey_info <- survey_info |>
    dplyr::select(
      id_project,
      waterbody,
      reach_no,
      id_site,
      site_name,
      sdate,
      geom_pnt,
      latitude,
      longitude,
      id_survey,
      gear_type,
      seconds
    ) |>
    rename(survey_date = sdate)

  # return
  survey_info

}

#' @rdname fetch_info
#'
#' @export
#'
fetch_species_info <- function(
    x = NULL,
    ...,
    collect = FALSE
) {

  # check x is OK for use
  x_ok <- is.null(x) |
    inherits(x, "tbl_PqConnection") |
    inherits(x, "data.frame")
  if (!x_ok)
    stop("x must be an unevaluated query or tbl/data.frame", call. = FALSE)

  # grab the full list of all taxon information
  taxon_lu <- fetch_table("taxon_lu")

  # filter based on columns of x if provided
  if (!is.null(x)) {

    # grab names of all fields in x
    xcol <- colnames(x)

    # pull out main columns of x and collect if it's an unevaluated query
    target_cols <- c("scientific_name", "common_name")
    available_targets <- xcol[xcol %in% target_cols]
    xval <- x |> dplyr::select(dplyr::all_of(available_targets))
    if (inherits(x, "tbl_PqConnection"))
      xval <- xval |> collect()

    # and filter on these one at a time
    for (i in seq_along(available_targets)) {
      unique_xval <- xval |>
        dplyr::select(dplyr::all_of(available_targets[i])) |>
        unlist() |>
        unique()
      taxon_lu <- taxon_lu |>
        dplyr::filter(
          !!rlang::parse_expr(available_targets[i]) %in% !!unique_xval
        )
    }

  }

  # filters fields based on regex if provided
  if (methods::hasArg("pattern")) {

    # set defaults for grepl function and update with arg_list values
    arg_list <- list(...)
    args <- list(
      pattern = arg_list$pattern,
      ignore.case = FALSE,
      perl = FALSE,
      fixed = FALSE,
      useBytes = FALSE
    )
    args[names(arg_list)] <- arg_list

    # filter all string columns based on these settings
    string_cols <- c(
      "scientific_name",
      "common_name",
      "authority",
      "primary_discipline",
      "taxon_type"
    )
    for (i in seq_along(string_cols)) {
      taxon_lu <- taxon_lu |>
        dplyr::filter(
          grepl(
            x = !!rlang::parse_expr(string_cols[i]),
            pattern = !!args$pattern,
            ignore.case = !!args$ignore.case,
            perl = !!args$perl,
            fixed = !!args$fixed,
            useBytes = !!args$useBytes
          )
        )
    }

  }

  # collect data if required
  if (collect)
    taxon_lu <- taxon_lu |> collect()

  # and arrange by scientific name
  taxon_lu <- taxon_lu |>
    dplyr::arrange(scientific_name)

  # return
  taxon_lu

}
