import os
import logging
import shutil

def setup_logging(log_file_path):
    """Sets up logging to both a file and the console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()

def ensure_dir_exists(path):
    """Creates a directory if it does not already exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Created directory: {path}")

def check_storage(path, required_gb=1):
    """Checks for available disk space and logs a warning if it's low"""
    try:
        _, _, free = shutil.disk_usage(path)
        free_gb = free / (1024**3)
        logging.info(f"Available storage at '{path}': {free_gb:.2f} GB")
        if free_gb < required_gb:
            logging.warning(f"Available space ({free_gb:.2f} GB) is less than the recommended {required_gb} GB.")
    except Exception as e:
        logging.error(f"Could not check disk space at '{path}'. Error: {e}")