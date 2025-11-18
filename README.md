# telemetry_parser
Project for develop a parser for raw bulkstats before inserting into InfluxDB.
Files processed are only *.csv received in a specific folder (/input by default) and the generated *.csv files are placed in other folder (/output as default)

Bulkstat config file is needed with headers for reference inside of /config_file directory output of command: 'show configuration bulkstats'

vars_parser.yml contains fundamental vars for telemetry parser:

CONFIG_FILE: /shared/telemetry_parser/config_file/TOR-VPC-1.log --> 'show configuration bulkstats' for headers reference
WATCH_FOLDER: /shared/telemetry_parser/input/ --> folder to monitor for incoming raw bulkstats
DEST_CSV: /shared/telemetry_parser/output/ --> destination for genereated post processed bulkstat files
LOG_PATH: /shared/telemetry_parser/log/ --> folder for generate 'debug.log' file with logging messages of parser application
BULK_FILE_NUMBER: 2 --> number to indicate 'file <NUMBER>' insider of CONFIG_FILE