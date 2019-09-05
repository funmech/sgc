import logging

from google.cloud.bigquery import Client, Table
from google.cloud import bigquery

from .project import Info


logger = logging.getLogger(__name__)


class BQClient(Info, Client):
    """A client for querying a BigQuery dataset"""

    def __init__(self, dataset):
        super().__init__()
        self.dataset = dataset

    def _full_table_name(self, name):
        """Get the full table name (id) by its short name"""
        return "%s.%s.%s" % (self.project_id, self.dataset, name)

    def create_table(self, name, schema):
        """Create a table with definition"""
        table = Table(self._full_table_name(name), schema=schema)
        table = super().create_table(table)
        self.describe_table(table_name)

    def create_empty_table(name):
        """Create an empty table"""
        return super().create_table(self._full_table_name(name))

    def describe_table(self, name):
        """Get basic information of a table in storage"""
        table = self.get_table(self._full_table_name(name))
        # View table properties
        logger.debug("Table schema: {}".format(table.schema))
        logger.debug("Table description: {}".format(table.description))
        logger.debug("Table has {} rows".format(table.num_rows))

    def load_from_json(self, table_name, file_path, auto_detect=False):
        """Load data in new line delimited JSON to a table"""
        job_config = bigquery.LoadJobConfig(
            autodetect=auto_detect,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        full_table_name = self._full_table_name(table_name)
        with open(file_path, "rb") as sf:
            load_job = super().load_table_from_file(
                sf, full_table_name, job_config=job_config
            )
        logger.debug("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        logger.debug("Job finished.")
        destination_table = super().get_table(full_table_name)
        logger.debug("Loaded {} rows.".format(destination_table.num_rows))

    def insert(self, table_name, rows):
        """Insert a list of dict into a table

        Returns:
            Sequence[Mappings]:
                check retrun list of errors.
        """
        # super().insert_rows() needs all fields to be presented
        # values have to be JSON-compatible, so some Python objects may cause problems
        return super().insert_rows_json(self._full_table_name(table_name), rows)

    def browse_rows(self, table_name, field_names, start_index=0, max_rows=10):
        """Similar to select query, but treats it natively var API"""
        table = self.get_table(self._full_table_name(table_name))
        fields = [field for field in table.schema if field.name in field_names]
        rows = client.list_rows(
            table, start_index=start_index, selected_fields=fields, max_results=max_rows
        )
        # Print row data in tabular format
        format_string = "{!s:<16} " * len(field_names)
        logger.debug(format_string.format(*field_names))  # prints column headers
        for row in rows:
            logger.debug(format_string.format(*row))  # prints row data

    def select(self, table_name, fields, conditions=None):
        """Run a select query"""
        # full table name has to be escaped for running SQL
        statement = "SELECT %s FROM `%s`" % (
            ",".join(fields),
            self._full_table_name(table_name),
        )
        if conditions:
            statement += " %s" % ",".join(conditions)

        query_job = self.query(statement)
        return query_job.result()  # return an iterator

    def count(self, table_name, conditions=None):
        """Do count through query, so it should have all counts: buffer and storage"""
        try:
            return list(self.select(table_name, ["count(*) as total"], conditions))[0]["total"]
        except Exception as err:
            logger.debug(err)
            return None

    @staticmethod
    def print_row(row):
        """Print a row with field names and values"""
        for k, v in row.items():
            logger.debug(f"{k} = {v}")

    @staticmethod
    def print_rows(rows):
        """Print rows with numbers and field names and values"""
        count = 0
        for row in rows:
            count += 1
            logger.debug(f"{count}:")
            for k, v in row.items():
                logger.debug(f"\t{k} = {v}")
