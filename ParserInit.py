from TelemetryParser import TelemetryParser
import os
import sys
import yaml

def read_vars(vars_path):
    vars = {}
    with open(vars_path, 'r') as f:
        vars = yaml.safe_load(f)
    return vars

def print_config(config_vars):
    print("\nCurrent Configuration:")
    for key, value in config_vars.items():
        print(f"  {key} = {value}")
    print()

def start_app(config_vars):
    print("\nStarting the app with this configuration:")
    print_config(config_vars)
    telemetry_parser = TelemetryParser(config_vars)
    telemetry_parser.run()


if __name__ == "__main__":
    #Read configuration from YAML file provided as command line argument
    config_vars = read_vars(sys.argv[1])
    #Start telemetry parser application
    start_app(config_vars)
