import logging
import json

from google.cloud import datastore

from .project import Info


logger = logging.getLogger(__name__)


class DSClient(Info, datastore.Client):
    """A DataStrore client"""

    @staticmethod
    def sorted_print(item):
        for k, v in sorted(item.items()):
            logger.debug(f"{k} = {v}")

    def list_kinds(self, users=True):
        """List kinds

        :type users: bool
        :param user: if only list user defined kind. default is True
        """
        query = self.query(kind="__kind__")
        kinds = [entity.key.id_or_name for entity in query.fetch()]
        if users:
            return [k for k in kinds if not k.startswith("_")]
        return kinds

    def get_statistics(self, kind):
        """Get statistics of a Kind"""
        query = self.query(kind="__Stat_Kind__")
        query.add_filter("kind_name", "=", kind)
        try:
            statistics = list(query.fetch(limit=1))[0]
            self.sorted_print(statistics)
            return statistics
        except IndexError:
            # The kind does not exist
            logger.warning("The kind %s does not exist", kind)
            return {}

    def list_keys(self, kind, limit=10):
        """List keys of a Kind"""
        # get filter keys
        for ent in self.query(kind=kind).fetch(limit=limit):
            self.sorted_print(ent)

    def download(self, *path_args):
        """Download entities based on a key's path

        :type path_args: tuple
        :param path_args: A tuple from positional arguments. Should be
                          alternating list of kinds (string) and ID/name
                          parts (int or string).
        """
        if len(path_args) == 0:
            logger.error("Key path must not be empty.")
            return []

        key = self.key(*path_args)
        return self.get(key)
