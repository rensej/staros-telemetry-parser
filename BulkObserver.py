from watchdog.events import FileSystemEventHandler
import os
import re
from pathlib import Path
import time

class CSVHandler(FileSystemEventHandler):

    def __init__(self, logger, headers, dest_bulk):
        self.logger = logger
        # build fast lookup: key -> header line (key is first 3 CSV fields joined by commas)
        self.header_map = headers
        self.dest_bulk = Path(dest_bulk)
        self.filename_re = re.compile(r"^(.+)_bulkstats_(.+)", re.IGNORECASE)

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

        try:
            with filepath.open('r', encoding='utf-8') as bulk_file:
                self.logger.info("Start processing lines...")
                for line in bulk_file:
                    # get first 3 csv fields
                    parts = line.split(',', 3)
                    if len(parts) < 3:
                        continue
                    key = ','.join(parts[:3])
                    header = self.header_map.get(key)
                    if not header:
                        continue
                    lines_processed += 1
                    # construct destination filename
                    safe_key = key.replace(',', '_')
                    out_name = f"{hostname}_{safe_key}_{timestamp}.csv"
                    out_path = str(self.dest_bulk / out_name)

                    # open file handle once per destination file
                    fh = open_files.get(out_path)
                    if fh is None:
                        # create parent if needed
                        self.dest_bulk.mkdir(parents=True, exist_ok=True)
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

        self.logger.info(f"Process complete! {lines_processed} matching lines processed")