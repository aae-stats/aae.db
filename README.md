## `aae.db`: methods to query the AAEDB

The `aae.db` package is a suite of methods to query and download data from the AAEDB. The current version of this package supports direct downloads of tables from a specified schema. Future extensions will include methods to process custom queries on the AAEDB and download the outputs of these queries.

## Usage

The package is currently available on GitHub and can be installed with the `remotes` package:

```
remotes::install_github("aae-stats/aae.db")
```

The current function to download data from the AAEDB is `fetch_table`, which returns a table by name from the AAEDB. By default, this function assumes data are in the `aquatic.data` schema (catalogue) but an alternative schema can be specified. This function returns the data set as a tibble.

The AAEDB uses an AWS back-end and has two access requirements. First, an AWS VPN is required (with appropriate login). Second, access to the AAEDB is controlled by a username and password (RStudio will prompt for these). You will not be able to access the AAEDB if you do not have login credentials for the AWS VPN or the AAEDB.

```
# download a flat table of VEFMAP data 
vefmap <- fetch_table("v_vefmap_only_flat_data")
```

## Contact

Please leave feedback, bug reports, or feature requests at the GitHub [issues page](https://github.com/aae-stats/aae.db/issues).

Last updated: 1 September 2022

