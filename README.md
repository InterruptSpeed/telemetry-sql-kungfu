# telemetry-sql-kungfu

This repo contains fake telemetry data and uses ANSI SQL kungfu to determine the time each vehicle is in a particular state. Ideas for future possibilities are found at the bottom of the sql file.

## Usage

- load the telemetry.csv file into your favourite database-like tool [Databricks SQL was used during development].
- execute the kungfu.sql script and adjust as necessary if ANSI SQL isn't compatible.

### Note

- common table expressions are used heavily in the sql to logically build up a readable script. It could also be adjusted to simply create a view that would be easy to query from without being exposed to all the kungfu.