"""
Logging utilities for the futures data manager.
"""

import sys
from pathlib import Path
from loguru import logger
from typing import Optional, Union


def setup_logging(
    log_file: Optional[Union[str, Path]] = None,
    log_level: str = "INFO",
    console_output: bool = True,
    file_rotation: str = "10 MB",
    file_retention: str = "30 days"
) -> None:
    """
    Set up comprehensive logging for the futures data manager.
    
    Args:
        log_file: Path to log file (optional)
        log_level: Logging level
        console_output: Whether to output to console
        file_rotation: File rotation policy
        file_retention: File retention policy
    """
    # Remove default handler
    logger.remove()
    
    # Console handler
    if console_output:
        logger.add(
            sys.stdout,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                   "<level>{message}</level>",
            colorize=True
        )
    
    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            log_path,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                   "{name}:{function}:{line} | {message}",
            rotation=file_rotation,
            retention=file_retention,
            compression="zip"
        )


def get_logger(name: str) -> "logger":
    """Get a logger instance for a specific module."""
    return logger.bind(name=name)


def configure_module_logging(
    module_name: str,
    log_level: str = "INFO"
) -> None:
    """
    Configure logging for a specific module.
    
    Args:
        module_name: Name of the module
        log_level: Logging level for this module
    """
    # This would be used to set different log levels for different modules
    # Implementation depends on specific logging requirements
    pass


def log_function_entry(func_name: str, **kwargs) -> None:
    """Log function entry with parameters."""
    args_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    logger.debug(f"Entering {func_name}({args_str})")


def log_function_exit(func_name: str, result=None) -> None:
    """Log function exit with result."""
    if result is not None:
        logger.debug(f"Exiting {func_name}, result: {type(result).__name__}")
    else:
        logger.debug(f"Exiting {func_name}")


def log_performance(func_name: str, duration: float) -> None:
    """Log performance timing."""
    logger.info(f"{func_name} completed in {duration:.3f}s")


class LoggerMixin:
    """Mixin class to add logging capabilities to any class."""
    
    @property
    def logger(self):
        """Get logger instance for this class."""
        return logger.bind(name=self.__class__.__name__)
    
    def log_info(self, message: str, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(message, **kwargs)
    
    def log_warning(self, message: str, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(message, **kwargs)
    
    def log_error(self, message: str, **kwargs) -> None:
        """Log error message with context."""
        self.logger.error(message, **kwargs)
    
    def log_debug(self, message: str, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(message, **kwargs)


def setup_structured_logging(
    service_name: str = "futures_data_manager",
    service_version: str = "1.0.0",
    environment: str = "development"
) -> None:
    """
    Set up structured logging with service context.
    
    Args:
        service_name: Name of the service
        service_version: Version of the service
        environment: Environment (development, staging, production)
    """
    # Bind context to all log messages
    logger.configure(
        extra={
            "service": service_name,
            "version": service_version,
            "environment": environment
        }
    )


# Performance logging decorator
def log_execution_time(logger_instance=None):
    """
    Decorator to log function execution time.
    
    Args:
        logger_instance: Logger instance to use (optional)
    """
    import functools
    import time
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            log_instance = logger_instance or logger
            log_instance.debug(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                log_instance.info(f"{func.__name__} completed in {execution_time:.3f}s")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                log_instance.error(f"{func.__name__} failed after {execution_time:.3f}s: {e}")
                raise
                
        return wrapper
    return decorator


# Error logging decorator  
def log_exceptions(logger_instance=None, reraise=True):
    """
    Decorator to log exceptions with full context.
    
    Args:
        logger_instance: Logger instance to use (optional)
        reraise: Whether to reraise the exception after logging
    """
    import functools
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log_instance = logger_instance or logger
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_instance.exception(
                    f"Exception in {func.__name__}: {e}",
                    func=func.__name__,
                    args=str(args)[:100],  # Truncate long args
                    kwargs=str(kwargs)[:100]
                )
                
                if reraise:
                    raise
                return None
                
        return wrapper
    return decorator


def setup_file_logging(
    log_dir: Union[str, Path],
    service_name: str = "futures_data_manager",
    max_size: str = "100 MB",
    retention: str = "1 month"
) -> None:
    """
    Set up file-based logging with rotation.
    
    Args:
        log_dir: Directory for log files
        service_name: Service name for log files
        max_size: Maximum file size before rotation
        retention: How long to keep old log files
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Application log
    logger.add(
        log_path / f"{service_name}.log",
        rotation=max_size,
        retention=retention,
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message}",
        level="DEBUG"
    )
    
    # Error-only log
    logger.add(
        log_path / f"{service_name}_errors.log",
        rotation=max_size,
        retention=retention,
        compression="zip", 
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message}",
        level="ERROR"
    )
    
    # Performance log
    logger.add(
        log_path / f"{service_name}_performance.log",
        rotation=max_size,
        retention=retention,
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {message}",
        filter=lambda record: "performance" in record["extra"],
        level="INFO"
    )


def get_performance_logger():
    """Get a logger specifically for performance metrics."""
    return logger.bind(performance=True)


# Context managers for logging
class LogContext:
    """Context manager for structured logging context."""
    
    def __init__(self, **context):
        self.context = context
        self.token = None
    
    def __enter__(self):
        self.token = logger.contextualize(**self.context)
        return self.token
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.token:
            self.token.__exit__(exc_type, exc_val, exc_tb)


class TimedLogContext:
    """Context manager for timed operations."""
    
    def __init__(self, operation_name: str, log_level: str = "INFO"):
        self.operation_name = operation_name
        self.log_level = log_level
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        logger.log(self.log_level, f"Starting {self.operation_name}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time if self.start_time else 0
        
        if exc_type:
            logger.error(f"{self.operation_name} failed after {duration:.3f}s: {exc_val}")
        else:
            logger.log(self.log_level, f"{self.operation_name} completed in {duration:.3f}s")