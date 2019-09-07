import logging


def set_demo_logger(logger):
    """Set loggers of demo and library"""
    logging.basicConfig(
        format="%(levelname)s: %(lineno)d - %(message)s",
    )
    logger.setLevel(logging.DEBUG)

    # set `gcloud_clients`'s logging level to debug
    gclients_logger = logging.getLogger('gcloud_clients')
    gclients_logger.propagate = False
    gclients_logger.setLevel(logging.DEBUG)

    hdl = logging.StreamHandler()
    hdl.setFormatter(logging.Formatter("%(levelname)s: %(name)s (%(lineno)d): %(message)s"))
    gclients_logger.addHandler(hdl)
