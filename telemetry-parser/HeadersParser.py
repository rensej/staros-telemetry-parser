import re
import os
import json

class HeadersParserBulk:

    def __init__(self, vars, logger=None):
        self.logger = logger
        self.vars = vars
        self.config_files_path = self.list_config_files()
        self.headers = self.headers_init()
        
    def list_config_files(self):
        #Lists CP and UP config files in a given directory, excluding YML files
        config_files_path = {}
        for filename in os.listdir(self.vars["CONFIG_FOLDER"]):
            full_path = os.path.join(self.vars["CONFIG_FOLDER"], filename)
            if os.path.isfile(full_path) and not filename.lower().endswith(('.yml', '.yaml')):
                config_files_path[os.path.splitext(filename)[0]] = full_path            
        return config_files_path
    
    def headers_init (self):
        #Header dict for headers for each file number in each config file
        header_map = {}
        copy_lines = False
        end_copy_lines = "#exit"      
        file_pattern = re.compile(r"^\s+file\s(\d+)")
        schema_pattern = re.compile(r"^\s+.+\sschema\s.+\sformat (.+)")
        file_number = 1

        #Generate header_map for each config file
        for type in self.config_files_path:
            #Initialize header_map for each config file type
            header_map[type] = {}
            with open(self.config_files_path[type], mode='r', newline='', encoding='utf-8') as config_file:
                #Initialize header_map CP and UP file number keys i.e for 'file 1' in CP --> header_map_CP = {'1':''}
                #Check if it is either CP or UP file config 
                for row in config_file:
                    #Match line with "file <bulk_file_number>" to recoginze file for bulkstat
                    line_file_match = re.search(file_pattern, row)
                    if line_file_match:
                        file_number = line_file_match.group(1)
                        #Initialize dict for each file number found in each config file type
                        header_map[type][file_number] = {}
                        copy_lines = True
                        if self.logger:
                            self.logger.info(f"📄 Found file {file_number} in config file for {type}, parsing headers...")

                    #If copy_lines=True File <number> was found and will start headers copying
                    if copy_lines:
                    #Headers filter remove unused spaces, only lines that starts with i.e."      AAA schema BBB format "
                        line = re.search(schema_pattern, str(row))
                        if line:
                            line_filter= line.group(1)
                            # Extract first 3 fields from header line
                            parts = line_filter.split(',', 3)
                            key = ','.join(parts[:3]) if len(parts) >= 3 else line_filter
                            # Keep the first header seen for a given key
                            header_map[type][file_number].setdefault(key, line_filter)
                    
                    #Check for end of headers copying for actual file number    
                    if end_copy_lines in row and copy_lines:
                        if self.logger:
                            self.logger.info(f"✅ Headers parsing completed for {type} and file {file_number} with {len(header_map[type][file_number])} entries.")
                            #Reset copy_lines for next file number
                            copy_lines = False
                        # break
                if len(header_map) == 0:
                    self.logger.error(f"🛑 No valid file configs found in  {self.vars["CONFIG_FOLDER"]}!")

        return header_map