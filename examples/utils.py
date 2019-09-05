import logging


def set_demo_logger(logger):
    """Set demo code's logger to DEBUG and scripting log format"""
    logging.basicConfig(
        format="%(levelname)s: %(lineno)d %(message)s",
    )
    logger.setLevel(logging.DEBUG)
