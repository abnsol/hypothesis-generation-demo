import os
import sys
from loguru import logger

def setup_logging(log_level='DEBUG'):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_path = os.path.join(log_dir, "app.log")
    log_file_jsonl = os.path.join(log_dir, "app.jsonl")
    error_log_path = os.path.join(log_dir, "error.log")
    error_log_jsonl = os.path.join(log_dir, "error.jsonl")

    logger.remove()

    # Console logging
    logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level=log_level, enqueue=True)

    # .log files with rotation, retention, and zip compression
    logger.add(log_file_path, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level=log_level, rotation="100 MB", retention="7 days", compression="zip", enqueue=True)
    logger.add(error_log_path, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="ERROR", rotation="100 MB", retention="7 days", compression="zip", enqueue=True)

    # .jsonl files with rotation, retention, and zip compression
    logger.add(log_file_jsonl, level=log_level, rotation="120 MB", retention="7 days", compression="zip", serialize=True, enqueue=True)
    logger.add(error_log_jsonl, level="ERROR", rotation="120 MB", retention="7 days", compression="zip", serialize=True, enqueue=True)