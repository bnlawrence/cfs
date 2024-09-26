# test_my_module.py
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def test_logging():
    logger.debug("Starting test")

