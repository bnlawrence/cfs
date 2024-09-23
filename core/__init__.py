import logging

# Get the logger for the core package
logger = logging.getLogger(__name__)

# Check if the logger already has handlers (to avoid adding multiple handlers)
if not logger.hasHandlers():
    handler = logging.StreamHandler()  # Set up a console stream handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)  # Attach the handler to the logger
    logger.setLevel(logging.DEBUG)  # Ensure the logger level is set to DEBUG