import logging
import utils

logger = logging.getLogger(__name__)
utils.set_demo_logger(logger)

from gcloud_clients.bigquery_c import BQClient


def demo_client_of_dataset(dataset):
    client = BQClient(dataset)
    for table in client.get_tables():
        client.describe_table(table.table_id)


if __name__ == "__main__":
    # client = BQClient(None)
    # datasets = list(client.list_datasets())
    # for dataset in datasets:
    #     logger.debug("dataset id = %s", dataset.dataset_id)
    #     demo_client_of_dataset(dataset.dataset_id)

    client = BQClient("save_predictions_test_one")
    rows = client.select("all_in_one", ("organisation_uid", "predicted_at"))
    client.print_rows(rows)
    client.browse_rows("all_in_one", ["organisation_uid", "predicted_at"], max_rows=2)

    # schema = client.schema_from_json("../combined_schema.json")
    # client.create_table("test_script", schema)
    # client.load_from_json("test_script", "InvoicePayablePredictions_nd.json")

    # read a list of dict from a json file
    # import json
    # with open("gcloud_clients/all_in_one.json", "rt") as jf:
    #     rows = json.load(jf)
    # logger.debug(client.insert("all_in_one", rows))
    # client.describe_table("all_in_one")
    # logger.debug(client.count("all_in_one"))
