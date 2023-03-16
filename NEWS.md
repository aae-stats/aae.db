# aae.db (development version)


# aae.db 0.1.0

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


# aae.db 0.0.1

## API changes

- Set default to collect data within fetch_ functions but added note
    that this default behaviour will reverse in the 0.1.0 release
- Add reexports for collect function and pipe method

## Fixes

- Included rstudioapi as an explicit import


# aae.db 0.0.1 

## Features

- Add aaedb_connect method to connect to database
- Include automatic methods to close connection when R session ends
- Use keyring to set auth credentials in keychain

