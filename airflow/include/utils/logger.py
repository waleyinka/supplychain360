# airflow/include/utils/logger.py

"""Shared logging configuration for SupplyChain360."""

import logging
import logging.handlers
import sys
import os

def get_logger(name: str) -> logging.Logger:
    """Configures and returns a logger with console and rotating file handlers.

    Args:
        name: The name of the logger

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if the logger is already configured
    if logger.handlers:
        return logger
    
    # Read log level from environment
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | [line:%(lineno)d] | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # File Handler: Rotates at midnight, keeps 7 days of logs
    log_dir = os.getenv("LOG_DIRECTORY", "logs")
    log_file = os.getenv("LOG_FILE_NAME", "supplychain360.log")
    
    try:
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, log_file)
        
        file_handler = logging.handlers.TimedRotatingFileHandler(
            file_path, when="midnight", backupCount=7, encoding="utf8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    except Exception as e:
        print(f"Warning: Could not configure file logging: {e}")
    
    logger.propagate = False
    return logger