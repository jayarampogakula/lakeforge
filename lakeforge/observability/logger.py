"""
LakeForge Logger Module
Provides structured logging with context tracking for data pipeline operations.
"""
import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
import json


class LakeForgeLogger:
    """
    Centralized logging utility for LakeForge operations.
    Provides structured logging with metadata tracking.
    """
    
    def __init__(
        self, 
        name: str = "lakeforge",
        level: str = "INFO",
        log_to_file: bool = False,
        log_file_path: Optional[str] = None
    ):
        """
        Initialize logger with specified configuration.
        
        Args:
            name: Logger name
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_file: Whether to log to file
            log_file_path: Path to log file if log_to_file is True
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        
        # Formatter with timestamp and context
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if requested
        if log_to_file and log_file_path:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        self.context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs):
        """Set context metadata for subsequent log messages."""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Clear all context metadata."""
        self.context.clear()
    
    def _format_message(self, message: str) -> str:
        """Format message with context if available."""
        if self.context:
            context_str = " | ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{message} | {context_str}"
        return message
    
    def info(self, message: str, **kwargs):
        """Log info message with optional metadata."""
        if kwargs:
            self.set_context(**kwargs)
        self.logger.info(self._format_message(message))
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional metadata."""
        if kwargs:
            self.set_context(**kwargs)
        self.logger.debug(self._format_message(message))
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional metadata."""
        if kwargs:
            self.set_context(**kwargs)
        self.logger.warning(self._format_message(message))
    
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception and metadata."""
        if kwargs:
            self.set_context(**kwargs)
        error_msg = self._format_message(message)
        if exception:
            error_msg += f" | Exception: {str(exception)}"
        self.logger.error(error_msg)
    
    def critical(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log critical message with optional exception and metadata."""
        if kwargs:
            self.set_context(**kwargs)
        critical_msg = self._format_message(message)
        if exception:
            critical_msg += f" | Exception: {str(exception)}"
        self.logger.critical(critical_msg)
    
    def log_operation(self, operation: str, status: str, **metadata):
        """
        Log a pipeline operation with structured metadata.
        
        Args:
            operation: Name of the operation (e.g., "csv_ingestion", "dq_validation")
            status: Status of the operation (e.g., "started", "completed", "failed")
            **metadata: Additional metadata to log
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "status": status,
            **metadata
        }
        self.info(f"Operation: {operation} | Status: {status}", **metadata)


# Singleton instance
_default_logger: Optional[LakeForgeLogger] = None


def get_logger(
    name: str = "lakeforge",
    level: str = "INFO",
    log_to_file: bool = False,
    log_file_path: Optional[str] = None
) -> LakeForgeLogger:
    """
    Get or create a logger instance.
    
    Args:
        name: Logger name
        level: Logging level
        log_to_file: Whether to log to file
        log_file_path: Path to log file
        
    Returns:
        LakeForgeLogger instance
    """
    global _default_logger
    if _default_logger is None:
        _default_logger = LakeForgeLogger(name, level, log_to_file, log_file_path)
    return _default_logger
