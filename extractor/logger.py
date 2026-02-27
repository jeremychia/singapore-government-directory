"""
Centralized logging configuration for the Singapore Government Directory Extractor.

Usage:
    from logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.debug("Detailed debug info")
    logger.error("An error occurred", exc_info=True)
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Create logs directory if it doesn't exist
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Log file with timestamp
LOG_FILE = LOGS_DIR / f"extractor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Custom formatter with colors for terminal output
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels."""
    
    COLORS = {
        logging.DEBUG: "\033[36m",     # Cyan
        logging.INFO: "\033[32m",      # Green
        logging.WARNING: "\033[33m",   # Yellow
        logging.ERROR: "\033[31m",     # Red
        logging.CRITICAL: "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    
    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(verbose: bool = False) -> None:
    """
    Set up logging configuration for the entire application.
    
    Args:
        verbose: If True, sets console logging to DEBUG level.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler (INFO by default, DEBUG if verbose)
    console_handler = logging.StreamHandler(sys.stdout)
    console_level = logging.DEBUG if verbose else logging.INFO
    console_handler.setLevel(console_level)
    console_format = ColoredFormatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)-25s │ %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    # File handler (always DEBUG level for full logging)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)-30s │ %(funcName)-20s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("pandas_gbq").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


# Convenience function to log function entry/exit
class LogContext:
    """Context manager for logging function entry and exit with timing."""
    
    def __init__(self, logger: logging.Logger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None
        
    def __enter__(self):
        self.start_time = datetime.now()
        details = ", ".join(f"{k}={v}" for k, v in self.kwargs.items())
        if details:
            self.logger.info(f"Starting: {self.operation} ({details})")
        else:
            self.logger.info(f"Starting: {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        if exc_type is not None:
            self.logger.error(
                f"Failed: {self.operation} after {duration:.2f}s - {exc_type.__name__}: {exc_val}"
            )
        else:
            self.logger.info(f"Completed: {self.operation} in {duration:.2f}s")
        return False  # Don't suppress exceptions
