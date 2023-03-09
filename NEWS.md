# aae.db (development version)

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

