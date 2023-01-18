## `aae.db`: methods to query the AAEDB

The `aae.db` package is a suite of methods to query and download data from the AAEDB. The current version of this package supports direct downloads of tables from the AAEDB as well as custom queries on the AAEDB.

## Usage

The package is currently available on GitHub and can be installed with the `remotes` package:

```
remotes::install_github("aae-stats/aae.db")
```

The main functions to download data from the AAEDB are `fetch_table`, which return tables by name from the AAEDB, and `fetch_query`, which returns custom queries specified as a SQL string or an R function. By default, these functions assume data are in the `aquatic.data` schema (catalogue) but an alternative schema can be specified. These functions return data sets as a single tibble.

The AAEDB uses a Microsoft Azure back-end, which requires a connection via the GoConnect VPN as well as a username and password (RStudio will prompt for these). You will not be able to access the AAEDB if you do not have login credentials for GoConnect or the AAEDB.

```
# download a flat table of VEFMAP data (single download, prompts for credentials each time) 
vefmap <- fetch_table("v_vefmap_only_flat_data")

# optional: store credentials to avoid re-entering for every connection
#   (will prompt for username and password)
# aaedb_key_set()

# connect once to the AAEDB and remain connected
aaedb_connect()

# process a simple SQL query to list all projects with data from the
#   Ovens river
survey_info <- fetch_query(
  "SELECT waterbody, id_project
   FROM aquatic_data.site a LEFT JOIN aquatic_data.survey b
   ON a.id_site = b.id_site
   WHERE lower(waterbody) LIKE 'ovens%'
   GROUP BY waterbody, id_project
   ORDER by waterbody, id_project"
   )

# process a dplyr query to collect site information
query_fn <- function(x) {
  tbl(x, in_schema(sql("aquatic_data"), sql("site"))) %>%
  filter(waterbody == "Ovens River")
}
ovens_sites <- fetch_query(query_fn)

# optional: disconnect from the AAEDB (automatically happens when the R session ends)
# aaedb_disconnect()
```

## Contact

Please leave feedback, bug reports, or feature requests at the GitHub [issues page](https://github.com/aae-stats/aae.db/issues).

Last updated: 19 January 2023 

