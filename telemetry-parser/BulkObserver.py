from watchdog.events import FileSystemEventHandler
import os
import re
from pathlib import Path
import time
import yaml
from HeadersParser import HeadersParserBulk

class CSVHandler(FileSystemEventHandler):

    def __init__(self, logger, vars):
        self.logger = logger
        # build fast lookup: key -> header line (key is first 3 CSV fields joined by commas)
        self.header_map = HeadersParserBulk(vars, logger).headers
        self.vars = vars
        self.filename_re = re.compile(r"^(.+)_bulkstats_(.+)", re.IGNORECASE)
        # self.inventory = self.read_inventory()

    def read_inventory(self):
        inventory = {}
        with open(rf"{self.vars["CONFIG_FOLDER"]}{self.vars["INVENTORY_NAME"]}", 'r') as f:
            inventory = yaml.safe_load(f)
        return inventory

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            self.logger.info(f"📄 New CSV file detected: {event.src_path}")
            # small delay to allow file write to complete
            time.sleep(0.5)
            self.process_csv(event.src_path)

    def process_csv(self, filepath):
        filepath = Path(filepath)
        basename = filepath.name
        m = self.filename_re.match(basename)
        hostname, timestamp = m.group(1), m.group(2)
        open_files = {}  # path_str -> file handle
        lines_processed = 0
        dest_bulk = Path(self.vars["DEST_CSV"])
        
        # Safe lookups to avoid KeyError if hostname not in inventory
        actual_inventory = self.read_inventory()
        host_info = actual_inventory.get(hostname)
        if not host_info:
            self.logger.warning(f"❌ Unknown host in inventory: {hostname}")
            self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
            return
         #Validate header table is not empy and exists for host, check TYPE and BULK_FILE_NUMBER
        if len(self.header_map) == 0:
            #Try to reload header map if empty
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers
            if len(self.header_map) == 0:
                self.logger.warning(f"❌ No headers found in header map.\nPlease add at least {actual_inventory[hostname].get("TYPE")}.log file in {self.vars["CONFIG_FOLDER"]} ")
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
        #Validate TYPE exist in header map
        elif actual_inventory[hostname].get("TYPE") not in self.header_map:
            self.logger.warning(f"❌ No header found for TYPE {actual_inventory[hostname].get("TYPE")} for {hostname} trying to reload header map.")
            #Try to reload header map if no header exists for TYPE
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers
            if actual_inventory[hostname].get("TYPE") not in self.header_map:
                self.logger.warning(f"❌ No header exist for TYPE {actual_inventory[hostname].get("TYPE")} for {hostname} please add the configuration file in {self.vars["CONFIG_FOLDER"]}")
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}") 
                return
            else:
                self.logger.info(f"✅ Header found for TYPE {actual_inventory[hostname].get("TYPE")} after reloading header map.")
        #Validate BULK_FILE_NUMBER exists in header map for TYPE
        elif str(actual_inventory[hostname].get("BULK_FILE_NUMBER")) not in self.header_map[actual_inventory[hostname].get("TYPE")]:
            self.logger.warning(f"❌ No header found for file  {actual_inventory[hostname].get("BULK_FILE_NUMBER")} for {hostname} trying to reload header map.")
            #Try to reload header map if  file not exists for TYPE
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers  
            if str(actual_inventory[hostname].get("BULK_FILE_NUMBER")) not in self.header_map[actual_inventory[hostname].get("TYPE")]:
                self.logger.warning(f"❌ No header exist for file {actual_inventory[hostname].get("BULK_FILE_NUMBER")} for {hostname} please add the configuration file in {self.vars["CONFIG_FOLDER"]}")
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
        #Star processing lines
        try:
            with filepath.open('r', encoding='utf-8') as bulk_file:
                self.logger.info("Start processing lines...")

                for line in bulk_file:
                    # get first 3 csv fields
                    parts = line.split(',', 3)
                    if len(parts) < 3:
                        continue
                    key = ','.join(parts[:3])           
                    
                    #Process line
                    header = self.header_map[actual_inventory[hostname].get("TYPE")][str(actual_inventory[hostname].get("BULK_FILE_NUMBER"))].get(key)
                    if not header:
                        continue
                    lines_processed += 1
                    # construct destination filename
                    safe_key = key.replace(',', '_')
                    out_name = f"{hostname}_{safe_key}_{timestamp}"
                    out_path = str(dest_bulk / out_name)

                    # open file handle once per destination file
                    fh = open_files.get(out_path)
                    if fh is None:
                        # create parent if needed
                        dest_bulk.mkdir(parents=True, exist_ok=True)
                        mode = 'a' if Path(out_path).exists() else 'w'
                        fh = open(out_path, mode, encoding='utf-8')
                        open_files[out_path] = fh
                        # write header only when creating file
                        if mode == 'w':
                            fh.write(header.rstrip('\n') + '\n')
                    fh.write(line)
        finally:
            # ensure all files are closed
            for fh in open_files.values():
                try:
                    fh.close()
                except Exception:
                    pass
        if lines_processed > 0:
            self.logger.info(f"Process complete! {lines_processed} matching lines processed")
            self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
