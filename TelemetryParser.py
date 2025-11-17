from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
import time
from HeadersParser import HeadersParserBulk
from LogParser import LogParserBulk
from BulkObserver import CSVHandler as CSVHandlerOptimized

class TelemetryParser:

    def __init__(self, vars):
        #Logging start
        self.log_parser = LogParserBulk(vars["LOG_PATH"]).log_parser
        #Observer
        self.vars_app = vars
        self.observer = self.initParser()

    def initParser(self):
        #Headers init list for bulkstat parsing
        headers_init = HeadersParserBulk(self.vars_app["CONFIG_FILE"], self.vars_app["BULK_FILE_NUMBER"], self.log_parser).headers
        if len(headers_init) == 0:
            self.log_parser.error("❌ No headers found, please check the configuration file and bulk file number.")
            exit(1)
        print(f"✅ Headers loaded: {len(headers_init)} headers found.")
        #CSV HanlderInit
        bulk_handler = CSVHandlerOptimized(self.log_parser, headers_init, self.vars_app["DEST_CSV"])
        #Start observer for the folder indicated in WATCH_FOLDER
        #Observer() for linux based VM
        # observer = Observer()
        #PollingObserver for Windows docker VM
        observer = PollingObserver()
        observer.schedule(bulk_handler, self.vars_app["WATCH_FOLDER"], recursive=False)
        observer.start()
        return observer
    
    def run(self):
        self.log_parser.info(f"👀 Watching for new CSV files in: {self.vars_app["WATCH_FOLDER"]}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log_parser.info("🛑 Stopping Telemetry Parser...")
            self.observer.stop()
        self.observer.join()