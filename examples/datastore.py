import logging
import json
import logging
import sys

import utils

sys.path.append("..")
logger = logging.getLogger(__name__)


from gcloud_clients.datastore_c import DSClient


if __name__ == "__main__":
    utils.set_demo_logger(logger)

    ds_client = DSClient()
    logger.debug(ds_client.list_kinds(False))
    logger.debug(ds_client.get_statistics("Invoice"))
    ds_client.list_keys("Account")
    data = ds_client.download("InvoicePayableRecurringPredictions", "0141j5tlqqrjijwu4mnayh")
    # logger.debug values
    for i in json.loads(data['predictions']):
        logger.debug(i)
