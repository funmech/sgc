import base64
import json
import logging
import os

from gcloud_clients.bigquery_c import BQClient


# set default logging level to INFO
logging.basicConfig(
    level=getattr(logging, os.environ.get("loglevel", "info").upper()),
    format="%(levelname)s: %(module)s %(lineno)d %(message)s",
)


def load(data):
    if not isinstance(data, list):
        logging.warning("BigQuery table only accept list of JSON")

    client = BQClient(os.environ["dataset"])
    rows = json.loads(data)
    logging.debug("To load %d rows" % len(rows))
    errors = client.insert(os.environ["table"], rows)
    if errors:
        logging.error("there was some errors")
        logging.error(errors)

    count = client.count(os.environ["table"])
    logging.info(
        "Successfully processed message. Total number of rows in BQ (includes buffer) now = %s",
        count,
    )


def pubsub_to_bq(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    logging.info(
        "This Function was triggered by messageId {} published at {}".format(
            context.event_id, context.timestamp
        )
    )

    if "data" in event:
        logging.debug(
            "Load to bigquery table %s.%s"
            % (os.environ["dataset"], os.environ["table"])
        )
        load(base64.b64decode(event["data"]))
