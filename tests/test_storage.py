import unittest
from unittest.mock import MagicMock, patch

import botocore

from yato.storage import Storage


class TestStorage(unittest.TestCase):
    def create_storage(self):
        return Storage(
            s3_access_key="access_key",
            s3_secret_key="secret_key",
            s3_endpoint_url="http://localhost:9000",
            s3_region_name="us-east-1",
        )

    def test_setup(self):
        storage = self.create_storage()

        self.assertEqual(storage.s3_access_key, "access_key")
        self.assertEqual(storage.s3_secret_key, "secret_key")
        self.assertEqual(storage.s3_endpoint_url, "http://localhost:9000")
        self.assertEqual(storage.s3_region_name, "us-east-1")

        self.assertIsInstance(storage.client, botocore.client.BaseClient)
        self.assertEqual(storage.client._endpoint.host, "http://localhost:9000")
        self.assertEqual(storage.client._client_config.region_name, "us-east-1")
        self.assertEqual(storage.client._request_signer._credentials.access_key, "access_key")
        self.assertEqual(storage.client._request_signer._credentials.secret_key, "secret_key")

    @patch("yato.storage.boto3.client")
    def test_download_folder(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "folder/file1"},
                {"Key": "folder/file2"},
            ]
        }

        storage = self.create_storage()
        storage.download_folder("mybucket", "folder", "dest_folder")
        mock_client.list_objects_v2.assert_called_once_with(Bucket="mybucket", Prefix="folder")

        expected_calls = [
            unittest.mock.call("mybucket", "folder/file1", "dest_folder/folder/file1"),
            unittest.mock.call("mybucket", "folder/file2", "dest_folder/folder/file2"),
        ]
        mock_client.download_file.assert_has_calls(expected_calls, any_order=True)

    @patch("yato.storage.boto3.client")
    @patch("os.listdir")
    def test_upload_folder(self, mock_os_listdir, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        storage = self.create_storage()

        mock_os_listdir.return_value = ["file1", "file2"]
        storage.upload_folder("mybucket", "source", "dest")
        expected_calls = [
            unittest.mock.call("source/file1", "mybucket", "dest/file1"),
            unittest.mock.call("source/file2", "mybucket", "dest/file2"),
        ]
        mock_client.upload_file.assert_has_calls(expected_calls, any_order=True)
