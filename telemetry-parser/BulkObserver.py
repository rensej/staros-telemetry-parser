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
        self.inventory = self.read_inventory()
        self.processing_pool = set() # Track files currently in flight

    def wait_for_file_ready(self, filepath, timeout=5):
        """
        Attempts to open the file in append mode to check if the OS has 
        released the write lock.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # If we can open it for appending, it's usually finished writing
                with open(filepath, 'a'):
                    return True
            except (IOError, OSError):
                # File is still being written to by another process
                time.sleep(0.2)
        return False

    def handle_file_event(self, filepath):
        """Unified entry point for both Created and Modified events"""
        if filepath in self.processing_pool:
            return # Already being handled, ignore duplicate trigger
        print("ENTRA")
        try:
            self.processing_pool.add(filepath)
            # A) If it's a CSV (Bulkstat data)
            if filepath.lower().endswith('.csv'):
                if os.path.commonpath([filepath, self.vars["WATCH_FOLDER"]]) in self.vars["WATCH_FOLDER"]:
                    self.logger.info(f"📄 New CSV file detected: {filepath}")
                    if self.wait_for_file_ready(filepath):
                        self.process_csv(filepath)
                    else:
                        self.logger.error(f"❌ Timeout: File {filepath} is still locked by another process.")
                        return
            # B) If it's a YAML/YML (Configuration or Inventory)
            elif filepath.lower().endswith(('.log', '.yaml', 'yml')):
                if os.path.commonpath([filepath, self.vars["CONFIG_FOLDER"]]) in self.vars["CONFIG_FOLDER"]:
                    self.logger.info(f"⚙️ Processing Config Change: {filepath}")
                    
                    if os.path.basename(filepath) in self.vars["INVENTORY_NAME"]:
                        self.logger.info(f"📋 Updating Inventory...")
                        self.inventory = self.read_inventory()
                        self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                    else:
                        self.logger.info(f"📜 Headers file modified reloading ...")
                        self.header_map = HeadersParserBulk(self.vars, self.logger).headers
                        self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
        finally:
            # Always remove from pool so the file can be processed again 
            # if a NEW version arrives later
            if filepath in self.processing_pool:
                self.processing_pool.remove(filepath)
        
    def read_inventory(self):
        inventory = {}
        with open(rf"{self.vars["CONFIG_FOLDER"]}{self.vars["INVENTORY_NAME"]}", 'r') as f:
            inventory = yaml.safe_load(f)
        return inventory

    def remove_file_if_exists(self, filepath):
        try:
            os.remove(filepath)
            self.logger.info(f"✅ Deleted file: {filepath}")
        except Exception as e:
            self.logger.warning(f"❌ Could not delete file: {filepath}. Error: {e}")

    def on_created(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path)
      
    def on_modified(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path)
    
    def process_csv(self, filepath):
        filepath = Path(filepath)
        basename = filepath.name
        m = self.filename_re.match(basename)
        hostname, timestamp = m.group(1), m.group(2)
        open_files = {}  # path_str -> file handle
        lines_processed = 0
        dest_bulk = Path(self.vars["DEST_CSV"])
        
        # Safe lookups to avoid KeyError if hostname not in inventory
        host_info = self.inventory.get(hostname)
        if not host_info:
            self.logger.warning(f"❌ Unknown host in inventory: {hostname} trying to reload inventory...")
            #Try to reload inventory if hostname not found
            # actual_inventory = self.inventory
            self.inventory = self.read_inventory()
            host_info = self.inventory.get(hostname)
            if not host_info:
                self.logger.warning(f"❌ Hostname {hostname} still not found in inventory. Please check the inventory file in {self.vars["CONFIG_FOLDER"]}")
                self.logger.info(f"❌ Deleting unprocessable file: {filepath}")
                self.remove_file_if_exists(filepath)
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
            else:
                self.logger.info(f"✅ Hostname {hostname} found in inventory after reloading...")
        else:
            self.logger.info(f"✅ Hostname {hostname} found in inventory")
         #Validate header table is not empy and exists for host, check TYPE and BULK_FILE_NUMBER
        if len(self.header_map) == 0:
            #Try to reload header map if empty
            self.logger.warning(f"❌ Header map is empty, trying to reload header map...")
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers
            if len(self.header_map) == 0:
                self.logger.warning(f"❌ No headers found in header map.\nPlease add at least {self.inventory[hostname].get("TYPE")}.log file in {self.vars["CONFIG_FOLDER"]} ")
                self.logger.info(f"❌ Deleting unprocessable file: {filepath}")
                self.remove_file_if_exists(filepath)
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
        #Validate TYPE exist in header map
        if self.inventory[hostname].get("TYPE") not in self.header_map:
            self.logger.warning(f"❌ No header found for TYPE {self.inventory[hostname].get("TYPE")} for {hostname} trying to reload header map...")
            #Try to reload header map if no header exists for TYPE
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers
            if self.inventory[hostname].get("TYPE") not in self.header_map:
                self.logger.warning(f"❌ No header exist for TYPE {self.inventory[hostname].get("TYPE")} for {hostname} please add the configuration file in {self.vars["CONFIG_FOLDER"]}")
                self.logger.info(f"❌ Deleting unprocessable file: {filepath}")
                self.remove_file_if_exists(filepath)
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
            else:
                self.logger.info(f"✅ Header found for TYPE {self.inventory[hostname].get("TYPE")} after reloading header map...")
        else:
            self.logger.info(f"✅ Header found for TYPE {self.inventory[hostname].get("TYPE")} checking if {self.inventory[hostname].get("BULK_FILE_NUMBER")} exists...")
        #Validate BULK_FILE_NUMBER exists in header map for TYPE
        if str(self.inventory[hostname].get("BULK_FILE_NUMBER")) not in self.header_map[self.inventory[hostname].get("TYPE")]:
            self.logger.warning(f"❌ No header found for file  {self.inventory[hostname].get("BULK_FILE_NUMBER")} for {hostname} trying to reload header map...")
            #Try to reload header map if  file not exists for TYPE
            self.header_map = HeadersParserBulk(self.vars, self.logger).headers  
            if str(self.inventory[hostname].get("BULK_FILE_NUMBER")) not in self.header_map[self.inventory[hostname].get("TYPE")]:
                self.logger.warning(f"❌ No header exist for file {self.inventory[hostname].get("BULK_FILE_NUMBER")} for {hostname} please add the configuration file in {self.vars["CONFIG_FOLDER"]}")
                self.logger.info(f"❌ Deleting unprocessable file: {filepath}")
                self.remove_file_if_exists(filepath)
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                return
        else:
            self.logger.info(f"✅ Header found for file {self.inventory[hostname].get("BULK_FILE_NUMBER")} for {hostname} proceeding to process file...")
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
                    header = self.header_map[self.inventory[hostname].get("TYPE")][str(self.inventory[hostname].get("BULK_FILE_NUMBER"))].get(key)
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
            else:
                self.logger.info("No matching lines found, nothing processed.")
                self.logger.info(f"👀 Watching for new CSV files in: {self.vars["WATCH_FOLDER"]}")
                self.remove_file_if_exists(filepath)

 



