import os

import boto3


class Storage:
    def __init__(self, s3_access_key, s3_secret_key, s3_endpoint_url, s3_region_name) -> None:
        """
        This class is a wrapper around boto3 S3 client.

        :param s3_access_key: The S3 access key.
        :param s3_secret_key: The S3 secret key.
        :param s3_endpoint_url: Endpoint URL of the S3 compatible storage.
        """
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_region_name = s3_region_name

        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            endpoint_url=self.s3_endpoint_url,
            region_name=self.s3_region_name,
        )

    def download_folder(self, bucket, source_folder, destination_folder) -> None:
        """
        This method downloads a folder from an S3 bucket.
        :return: None
        """
        files = [item["Key"] for item in self.client.list_objects_v2(Bucket=bucket, Prefix=source_folder)["Contents"]]
        for file in files:
            self.client.download_file(bucket, file, os.path.join(destination_folder, file))

    def upload_folder(self, bucket, source_folder, destination_folder) -> None:
        """
        This method uploads a folder to an S3 bucket.
        """
        for file in os.listdir(source_folder):
            filename = os.path.join(source_folder, file)
            self.client.upload_file(filename, bucket, os.path.join(destination_folder, file))
