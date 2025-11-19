from TelemetryParser import TelemetryParser
from LogParser import LogParserBulk
import os
import sys
import yaml

def read_vars(vars_path):
    vars = {}
    with open(vars_path, 'r') as f:
        vars = yaml.safe_load(f)
    return vars

def print_config(config_vars, log_parser):
    for key, value in config_vars.items():
        log_parser.info(f"  {key} = {value}")
    print()

def check_path_exists(path, description, log_parser):
    if not os.path.exists(path):
        log_parser.error(f"❌ {description} does not exist: {path}")
        exit(1)

def start_app(config_vars, log_parser):
    #Check if WATCH_FOLDER exists
    check_path_exists(config_vars["WATCH_FOLDER"], "WATCH_FOLDER", log_parser)

    #Check if CONFIG_FOLDER exists
    check_path_exists(config_vars["CONFIG_FOLDER"], "CONFIG_FOLDER", log_parser)

    #Create DEST_CSV folder if it does not exist
    if not os.path.exists(config_vars["DEST_CSV"]):
        os.makedirs(config_vars["DEST_CSV"])
        log_parser.info(f"✅ Created DEST_CSV folder: {config_vars["DEST_CSV"]}")  
         
    #Check if inventory file exists
    inventory_path = os.path.join(config_vars["CONFIG_FOLDER"], config_vars["INVENTORY_NAME"])
    check_path_exists(inventory_path, "Inventory file", log_parser)

    log_parser.info("\nStarting the app with this configuration:")
    print_config(config_vars, log_parser)
    telemetry_parser = TelemetryParser(config_vars, log_parser)
    telemetry_parser.run()


if __name__ == "__main__":
    #Read configuration from YAML file provided as command line argument
    config_vars = read_vars(sys.argv[1])
    log_parser = LogParserBulk(config_vars["LOG_PATH"]).log_parser
    #Start telemetry parser application
    start_app(config_vars, log_parser)
