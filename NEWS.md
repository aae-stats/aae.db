# aae.db (development version)

# aae.db 0.1.0.9000

## Features

- Add methods to extract site or species information either for all
    species in database or for a filtered subset: `fetch_site_info` and
    `fetch_species_info`
- Update `fetch_project` to optionally return multiple projects in a
    single flat table

## API changes

- Change default to lazy evaluation of queries (collect = FALSE)
- Update `fetch_project` function to use R-side manipulation of
    queries for greater flexibility

# aae.db 0.0.1.9003

## API changes

- Remove promise class and reexport dplyr collect method directly
- Set default to collect data within fetch_ functions but added note
    that this default behaviour will reverse in the 0.1.0 release
- Add reexports for collect function and pipe method

# aae.db 0.0.1.9002

## API changes

- Add aaedb_promise class to shift collect step outside of
    the fetch_ functions
- Add wrapper for S3 collect method to download data following
   filtering steps


# aae.db 0.0.1.9001

## Fixes

- Included rstudioapi as an explicit import


# aae.db 0.0.1 

## Features

- Add aaedb_connect method to connect to database
- Include automatic methods to close connection when R session ends
- Use keyring to set auth credentials in keychain

