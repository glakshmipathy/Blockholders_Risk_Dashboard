import logging
import os
import config

# Ensure logs directory exists
if not os.path.exists(config.LOGS_DIR):
    os.makedirs(config.LOGS_DIR)

def get_logger(name='risk_graph_logger') -> logging.Logger:
    """
    Returns a logger with the specified name.
    Ensures no duplicate handlers are added.
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers in hot reload scenarios (e.g., Streamlit)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Console Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File Handler
        log_file = os.path.join(config.LOGS_DIR, f'{name}.log')
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

# Export the default logger for global use
logger = get_logger()

__all__ = ['get_logger', 'logger']