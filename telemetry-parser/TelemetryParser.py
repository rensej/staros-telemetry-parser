from watchdog.observers import Observer
import time
from HeadersParser import HeadersParserBulk
from BulkObserver import CSVHandler as CSVHandlerOptimized

class TelemetryParser:

    def __init__(self, vars, log_parser=None):
        #Logging start
        self.log_parser = log_parser
        #Observer
        self.vars = vars
        self.observer = self.initParser()

    def count_total_headers(self, headers_dict):
        # Recursively counts all keys in a nested dictionary.
        total_headers = 0
        for key, value in headers_dict.items():
            if isinstance(value, dict):
                total_headers += len(value)
        return total_headers

    def initParser(self):
        #Headers init list for bulkstat parsing
        headers_init = HeadersParserBulk(self.vars, self.log_parser).headers
        if len(headers_init) == 0:
            self.log_parser.error(f"❌ No headers found, please check the configuration file in {self.vars["CONFIG_FOLDER"]}.")
            exit(1)
        self.log_parser.info(f"✅ Headers loaded: {self.count_total_headers(headers_init)} headers found.")
        #CSV HanlderInit
        bulk_handler = CSVHandlerOptimized(self.log_parser, headers_init, self.vars)
        #Start observer for the folder indicated in WATCH_FOLDER
        #Observer() for linux based VM
        observer = Observer()
        observer.schedule(bulk_handler, self.vars["WATCH_FOLDER"], recursive=False)
        observer.start()
        return observer
    
    def run(self):
        self.log_parser.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log_parser.info("🛑 Stopping Telemetry Parser...")
            self.observer.stop()
        self.observer.join()