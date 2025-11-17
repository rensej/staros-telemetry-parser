# telemetry_parser
Project for develop a parser for raw bulkstats before inserting into InfluxDB.
Files processed are only *.csv received in a specific folder (informed at startup or /input by default) and the generated *.csv files are placed in other folder (/output as default)

Bulkstat config file is needed with headers for reference inside of /config_file directory output of command: 'show configuration bulkstats'
