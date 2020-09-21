# Implog

implog is a quick and dirty golang program to import log files into a database for statistics and analytics.
It's mostly a toy to learn database usage under go.  At the moment nothing exists to read and display the database format in a useful form, but eventually I may get around to writing it.  Basic SQL queries can be written by hand for those who want to.

## Dependencies

* [go-sql-driver for mysql]: https://github.com/go-sql-driver/mysql
* [google uuid]: github.com/google/uuid
* A MySQL or MariaDB database

## Usage

```
implog --name <logname> --logdir <log directory> --dbconnection "<user>:<password>@tcp(<hostname>)/<dbname>"
```

The necessary database tables will be created (if they do not already exist).  The idea is to run the application from a cron job roughly once a day, or however often your log files are rotated.  Files that have already been read completely will be skipped and duplicate entries should be avoided (based on a hash).  This isn't as efficient as it could be, but only one file will need to be read more than once under most circumstances so the issue is minor for me.

Support for other databases is not currently planned but should be possible to implement cleanly if desired.

Log files are in basic access_log format.  Compressed log files (with gzip) will be detected and read in their compressed form.  Logfiles can be read in parallel, defaulting to four at a time, if a directory is specified.  Also if a directory is specified, files are expected to be prefixed with access_log.

Logs can be placed into separate databases easily (so each host can analyze only their logs) or can be placed into the same database with a logname to separate them.

