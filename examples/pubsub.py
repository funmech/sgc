import logging
import sys

import utils

sys.path.append("..")
logger = logging.getLogger(__name__)

from gcloud_clients.pubsub_c import PClient


# demo code
def list_project_topics(client):
    for topic in client.get_topics():
        logger.debug(str(topic).rstrip())


def publish_to(client, topic_name):
    messages = (f"Message number {n}" for n in range(1, 10))
    client.publish_messages(topic_name, messages)


if __name__ == "__main__":
    utils.set_demo_logger(logger)

    publisher = PClient()
    list_project_topics(publisher)
    # publish_to(publisher, "tempo-test")

    # client = SClient()
    # client.receive_messages("laptop")
