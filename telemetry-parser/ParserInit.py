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
        log_parser.warning(f"❌ {description} does not exist: {path}")
        os.makedirs(path, exist_ok=True)
        log_parser.info(f"✅ Creating {description}: {path}")
        # exit(1)
    else:
        log_parser.info(f"✅ {description} exists: {path}")

def start_app(config_vars, log_parser):
    #Create WATCH_FOLDER if it does not exist
    check_path_exists(config_vars["WATCH_FOLDER"], "WATCH_FOLDER", log_parser)

    #Create CONFIG_FOLDER if it does not exist 
    check_path_exists(config_vars["CONFIG_FOLDER"], "CONFIG_FOLDER", log_parser)

    #Create DEST_CSV folder if it does not exist
    check_path_exists(config_vars["DEST_CSV"], "DEST_CSV", log_parser)
         
    #Check if inventory file exists
    inventory_path = os.path.join(config_vars["CONFIG_FOLDER"], config_vars["INVENTORY_NAME"])
    if not os.path.isfile(inventory_path):
        log_parser.warning(f"❌ Inventory file does not exist: {inventory_path}")
        log_parser.warning("Creating default inventory template...")
        default_inventory_content = """# Default Inventory Template
# Please fill in the required fields
CP-COR01:
  TYPE: CP
  BULK_FILE_NUMBER: 2
UP-COR01:
  TYPE: UP
  BULK_FILE_NUMBER: 2
"""
        with open(inventory_path, 'w') as f:
            f.write(default_inventory_content)
            f.close()
        log_parser.info(f"✅ Default inventory template created at: {inventory_path}")
    else:
        log_parser.info(f"✅ Inventory file exists: {inventory_path}")
     #   exit(1)
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
