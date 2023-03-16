## `aae.db`: methods to query the AAEDB

The `aae.db` package is a suite of methods to query and download data from the AAEDB. The current version of this package supports direct downloads of tables from the AAEDB as well as customised queries, either using dplyr methods or SQL. Lazy evaluation is the default for all queries, which means pre-processing of queries occurs on the AAEDB rather than locally.


## Usage

The package is currently available on GitHub and can be installed with the `remotes` package:

```
remotes::install_github("aae-stats/aae.db")
```

The main function to download data from the AAEDB is `fetch_project`, which is recommended for most applications. The `fetch_project` function returns data for specific projects (see `?fetch_data` for a list of projects), and can return data for multiple projects simultaneously.

Alternative functions are `fetch_table`, which return tables by name from the AAEDB, and `fetch_query`, which returns custom queries specified as an SQL string. By default, the `fetch_table` function assumes data are in the `aquatic.data` schema (catalogue) but an alternative schema can be specified.

There are several helper functions to extract information on sites or species. This information can be filtered based on the sites or species in a downloaded data table, or with regex expressions. See `?fetch_data` for examples.

The AAEDB uses a Microsoft Azure back-end, which requires a connection via the GoConnect VPN as well as a username and password (RStudio will prompt for these). You will not be able to access the AAEDB if you do not have login credentials for GoConnect or the AAEDB.

```
# download a flat table of all VEFMAP data
#  (single download, prompts for credentials each time) 
vefmap <- fetch_table("v_vefmap_only_flat_data", collect = TRUE)

# optional: store credentials to avoid re-entering for every connection
#   (will prompt for username and password)
# aaedb_key_set()

# connect once to the AAEDB and remain connected
aaedb_connect()

# repeat this, but without evaluating the query
vefmap_lazy <- fetch_table("v_vefmap_only_flat_data")

# this lazy query can be manipulated prior to downloading the data,
#   for example, using the many dplyr methods
library(dplyr)
vefmap_lazy <- vefmap_lazy %>%
  filter(
    grepl("campaspe", waterbody, ignore.case = TRUE),
    scientific_name == "Maccullochella peelii"
  ) %>%
  select(
    waterbody, id_site, site_name, seconds, length_mm, collected, observed
  )
  
# download this data set
vefmap_lazy <- vefmap_lazy %>% collect()

# fetch information on the sites in a data set
vefmap_site_info <- fetch_site_info(vefmap_lazy)

# "spatialise" this information (requires the `sf` package)
library(sf)
vefmap_site_info <- vefmap_site_info %>%
  filter(!is.na(geom_pnt)) %>%
  collect()
vefmap_sf <- vefmap_site_info %>%
  st_set_geometry(st_as_sfc(vemfap_site_info$geom_pnt))
  
# and make a basic plot of this with the `mapview` package
library(mapview)
vefmap_sf %>%
  select(-geom_pnt) %>%
  mapview(col.regions = "DarkGreen", label = "site_name", layer.name = "Survey site")
    
# prepare a simple SQL query to list all projects with data from the
#   Ovens river
survey_info <- fetch_query(
  "SELECT waterbody, id_project
    FROM aquatic_data.site a LEFT JOIN aquatic_data.survey b
    ON a.id_site = b.id_site
    WHERE lower(waterbody) LIKE 'ovens%'
    GROUP BY waterbody, id_project
    ORDER by waterbody, id_project",
  collect = FALSE
)

# and collect this data set
survey_info <- survey_info %>% collect()

# use dplyr tools to download information on all
#   sites in the Ovens River
site_table <- fetch_table("site", collect = FALSE)
ovens_sites <- site_table %>%
  filter(grepl("ovens", waterbody, ignore.case = TRUE)) %>%
  collect()

# optional: disconnect from the AAEDB
#   (automatically happens when the R session ends)
# aaedb_disconnect()
```

## Contact

Please leave feedback, bug reports, or feature requests at the GitHub [issues page](https://github.com/aae-stats/aae.db/issues).

Last updated: 16 March 2023 

