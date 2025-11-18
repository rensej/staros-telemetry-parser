import logging
from logging.handlers import RotatingFileHandler

class LogParserBulk:

    def __init__(self, logPath):
        self.log_parser = self.log_events(logPath)

    def log_events(self, logPath):
        # Create logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # Create handlers
        file_handler = RotatingFileHandler(f"{logPath}/debug.log", maxBytes=50*1024, backupCount= 7)
        console_handler = logging.StreamHandler()

        # Set log levels
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.INFO)

        # Create formatters and add them
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger
