import unittest

from yato import Yato
from yato.storage import Storage


class TestYato(unittest.TestCase):
    def test_yato_storage(self):
        yato = Yato(
            database_path="path",
            sql_folder="folder",
            s3_bucket="mybucket",
            s3_access_key="access",
            s3_secret_key="secret",
        )
        self.assertIsInstance(yato.storage, Storage)

    def test_yato_storage_none(self):
        yato = Yato(
            database_path="path",
            sql_folder="folder",
        )
        self.assertIsNone(yato.storage)
