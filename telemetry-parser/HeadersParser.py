import re

class HeadersParserBulk:

    def __init__(self, headerspath, bulk_file_number, logger=None):
        self.logger = logger
        self.headerspath = headerspath
        self.headers = self.headers_init(bulk_file_number)

    def headers_init (self, bulk_file_number):
        # headers = []
        header_map = {}
        copy_lines = False
        end_copy_lines = "#exit"
        file_pattern = rf"file\s{bulk_file_number}"
        schema_pattern = re.compile(r"^\s+.+\sschema\s.+\sformat (.+)")

        with open(self.headerspath, mode='r', newline='', encoding='utf-8') as config_file:
            for row in config_file:
                #Match line with "file <bulk_file_number>" to recoginze file for bulkstat
                if re.search(file_pattern, row):
                    copy_lines = True
                    if self.logger:
                        self.logger.info(f"📄 Found file {bulk_file_number} in config file, parsing headers...")

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
                        header_map.setdefault(key, line_filter)

                if end_copy_lines in row and copy_lines:
                    if self.logger:
                         self.logger.info(f"✅ Headers parsing completed.")
                    break
            if not copy_lines:
                self.logger.error(f"🛑 File {bulk_file_number} NOT in config file!")
                    
        return header_map