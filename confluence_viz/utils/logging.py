"""
Logging utilities for Confluence visualization.
Provides structured logging with configurable levels and formats.
"""

import logging
import sys
from typing import Optional
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors to log messages for console output.
    """
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        # Add color to the level name
        if record.levelname in self.COLORS:
            colored_level = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
            record.levelname = colored_level
        
        return super().format(record)


def setup_logging(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    enable_colors: bool = True,
    logger_name: str = 'confluence_viz'
) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for logging output
        enable_colors: Whether to enable colored output for console
        logger_name: Name of the logger
        
    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(numeric_level)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    
    # Choose formatter based on color preference
    if enable_colors and sys.stdout.isatty():
        console_format = '%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d | %(message)s'
        console_formatter = ColoredFormatter(console_format, datefmt='%H:%M:%S')
    else:
        console_format = '%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d | %(message)s'
        console_formatter = logging.Formatter(console_format, datefmt='%Y-%m-%d %H:%M:%S')
    
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(numeric_level)
        
        file_format = '%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d | %(message)s'
        file_formatter = logging.Formatter(file_format, datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Name of the logger (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f'confluence_viz.{name}')


class ProgressLogger:
    """
    Helper class for logging progress of long-running operations.
    """
    
    def __init__(self, logger: logging.Logger, total: int, operation: str = "items"):
        """
        Initialize progress logger.
        
        Args:
            logger: Logger instance to use
            total: Total number of items to process
            operation: Description of what's being processed
        """
        self.logger = logger
        self.total = total
        self.operation = operation
        self.current = 0
        self.last_logged_percent = -1
    
    def update(self, increment: int = 1, message: Optional[str] = None) -> None:
        """
        Update progress and log if necessary.
        
        Args:
            increment: Amount to increment progress by
            message: Optional custom message to log
        """
        self.current += increment
        percent = int((self.current / self.total) * 100) if self.total > 0 else 0
        
        # Log every 10% or on custom message
        if percent >= self.last_logged_percent + 10 or message:
            if message:
                self.logger.info(f"Progress: {self.current}/{self.total} {self.operation} ({percent}%) - {message}")
            else:
                self.logger.info(f"Progress: {self.current}/{self.total} {self.operation} ({percent}%)")
            self.last_logged_percent = percent
    
    def finish(self, message: Optional[str] = None) -> None:
        """
        Log completion of the operation.
        
        Args:
            message: Optional completion message
        """
        if message:
            self.logger.info(f"Completed: {self.current}/{self.total} {self.operation} - {message}")
        else:
            self.logger.info(f"Completed: {self.current}/{self.total} {self.operation}")


def log_function_call(logger: logging.Logger):
    """
    Decorator to log function calls and their duration.
    
    Args:
        logger: Logger instance to use
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            
            # Log function entry
            logger.debug(f"Entering {func.__name__} with args={args[:3]}{'...' if len(args) > 3 else ''}, kwargs={list(kwargs.keys())}")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.debug(f"Completed {func.__name__} in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Failed {func.__name__} after {duration:.2f}s: {e}")
                raise
        
        return wrapper
    return decorator


# Default logger for the package
default_logger = setup_logging()


def set_log_level(level: str) -> None:
    """
    Set the log level for all confluence_viz loggers.
    
    Args:
        level: New log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Update all confluence_viz loggers
    for name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger) and name.startswith('confluence_viz'):
            logger.setLevel(numeric_level)
            for handler in logger.handlers:
                handler.setLevel(numeric_level)