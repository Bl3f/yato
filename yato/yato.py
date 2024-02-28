import logging
import os
import tempfile
from graphlib import TopologicalSorter

import duckdb

from yato.parser import get_dependencies, read_sql
from yato.storage import Storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Yato:
    def __init__(
        self,
        database_path: str,
        sql_folder: str,
        dialect: str = "duckdb",
        schema: str = "transform",
        db_folder_name: str = "db",
        s3_bucket: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
        s3_endpoint_url: str = None,
    ) -> None:
        """
        yato stands for yet another transformation orchestrator.

        The goal of yato is to provide the lightest SQL orchestrator on earth on top of DuckDB.
        You just need a bunch of SQL files in a folder, a configuration file, and you're good to go.
        Yato uses S3 compatible storage to backup and restore the DuckDB database between runs.

        :param database_path:  The path to the database file (reminder: only DuckDB is supported).
        :param sql_folder: The folder containing the SQL files to run.
        :param dialect: The SQL dialect to use in SQLGlot. Default is DuckDB and only DuckDB is supported.
        :param schema: The schema to use in the DuckDB database.
        :param db_folder_name: The name of the folder to use for backup and restore.
        :param s3_bucket: The S3 bucket to use for backup and restore.
        :param s3_access_key: The S3 access key.
        :param s3_secret_key: The S3 secret key.
        :param s3_endpoint_url: The S3 endpoint URL.
        """
        self.database_path = database_path
        self.sql_folder = sql_folder
        self.dialect = dialect
        self.schema = schema
        self.db_folder_name = db_folder_name
        self.s3_bucket = s3_bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint_url = s3_endpoint_url

    @property
    def storage(self) -> object or None:
        """
        Returns a boto3 S3 client if the S3 credentials are provided. Otherwise, returns None.
        :return: object or None
        """
        if self.s3_access_key and self.s3_secret_key and self.s3_bucket:
            return Storage(
                s3_access_key=self.s3_access_key,
                s3_secret_key=self.s3_secret_key,
                s3_endpoint_url=self.s3_endpoint_url,
            )
        return None

    def restore(self) -> None:
        """
        Restores the DuckDB database from the S3 bucket.
        """
        logger.info(f"Restoring the DuckDB database from {self.s3_bucket}/{self.db_folder_name}...")
        with tempfile.TemporaryDirectory() as tmp_dirname:
            local_db_path = os.path.join(tmp_dirname, self.db_folder_name)

            os.mkdir(local_db_path)
            self.storage.download_folder(self.s3_bucket, self.db_folder_name, tmp_dirname)
            con = duckdb.connect(self.database_path)
            con.sql(f"IMPORT DATABASE '{local_db_path}'")
        logger.info("Done.")

    def backup(self) -> None:
        """
        Backups the DuckDB database to the S3 bucket.
        """
        logger.info(f"Backing up the DuckDB database to {self.s3_bucket}/{self.db_folder_name}...")
        with tempfile.TemporaryDirectory() as tmp_dirname:
            local_db_path = os.path.join(tmp_dirname, self.db_folder_name)

            con = duckdb.connect(self.database_path)
            con.sql(f"EXPORT DATABASE '{local_db_path}' (FORMAT 'parquet')")
            self.storage.upload_folder(self.s3_bucket, local_db_path, self.db_folder_name)
        logger.info("Done.")

    def get_execution_order(self, dependencies):
        ts = TopologicalSorter(dependencies)
        return list(ts.static_order())

    def run_pre_queries(self, con):
        con.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        con.sql(f"USE {self.schema}")

    def run_queries(self, execution_order, con):
        for query_name in execution_order:
            filename = os.path.join(self.sql_folder, f"{query_name}.sql")
            if os.path.exists(filename):
                print(f"Running {query_name}...")
                self.run_query(filename, query_name, con)
                print(f"OK.")
            else:
                print(f"Identified {query_name} as a source.")

    def run_query(self, filename, table_name, con):
        sql = read_sql(filename)
        con.sql(f"""CREATE OR REPLACE TABLE {self.schema}.{table_name} AS {sql}""")

    def run(self) -> object:
        """
        Runs do all the magic, it parses all the SQL queries, resolves the dependencies,
        and runs the queries in the guessed order.

        :return: Then it returns a DuckDB connection object.
        """
        con = duckdb.connect(self.database_path)
        dependencies = get_dependencies(self.sql_folder, self.dialect)
        execution_order = self.get_execution_order(dependencies)
        self.run_pre_queries(con)
        self.run_queries(execution_order, con)

        return con
