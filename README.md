# telemetry_parser
Project for develop a parser for raw bulkstats before inserting into InfluxDB.
Files processed are only *.csv received in a specific folder (/input by default) and the generated *.csv files are placed in other folder (/output as default)

Bulkstat config file is needed with headers for reference inside of /config_file directory output of command: 'show configuration bulkstats'

vars_parser.yml contains fundamental vars for telemetry parser:

CONFIG_FOLDER: /staros-config/ --> Must contains at least one config file with 'show configuration bulkstats' for headers reference i.e: CP.cfg , UP.cfg UP_IMS.cfg (.yaml/.yml are excluded as config files)

WATCH_FOLDER: /collector-input/ --> folder to monitor for incoming raw bulkstats

DEST_CSV: /telegraf-input/--> destination folder for genereated post processed bulkstat files

LOG_PATH: /log/ --> folder for generate 'debug.log' file with logging messages of parser application

INVENTORY_NAME: inventory.yml --> file name for inventory that contains: hostnames of elements for post process raw bulkstats, type must match with config file added i.e: for CP.cfg type is CP and last file number to match specific headers for the raw file.
This file must be located in CONFIG_FOLDER 

Format:
CP-COR01:
  TYPE: CP
  BULK_FILE_NUMBER: 2
UP-COR01:
  TYPE: UP
  BULK_FILE_NUMBER: 2
UP-COR02:
  TYPE: UP
  BULK_FILE_NUMBER: 2


-----------

To start the application run following command indicating where vars.yml is located:

python ParserInit.py "/root/vars_parser.yml"