import json
import os
from typing import List

from google.cloud import storage  # type: ignore
from google.oauth2 import service_account  # type: ignore

from src import ROOT_PATH
from src.utils import config


def download(bucket_name: str, prefix: str = "", dst_folder: str = ""):
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(str(config.get("secret.gcp.storage.credentials"))),
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_or_name=bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        path = os.path.join(ROOT_PATH, dst_folder, "/".join(blob.name.split("/")[0:-1]))
        os.makedirs(path, exist_ok=True)
        blob.download_to_filename(os.path.join(ROOT_PATH, dst_folder, blob.name))


def download_str(bucket_name: str, prefix: str = "") -> List[str]:
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(str(config.get("secret.gcp.storage.credentials"))),
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_or_name=bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    data = []
    for blob in blobs:
        data.append(blob.download_as_text())

    return data


def upload(bucket_name: str, src_path: str, prefix: str = ""):
    src_path = os.path.normpath(src_path)
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(str(config.get("secret.gcp.storage.credentials"))),
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_or_name=bucket_name)

    def upload_file(path=""):
        if os.path.isdir(os.path.join(ROOT_PATH, src_path, path)):
            for file_name in os.listdir(os.path.join(ROOT_PATH, src_path, path)):
                upload_file(os.path.join(path, file_name))
        else:
            blob = bucket.blob("/".join(os.path.join(prefix, path).split(os.sep)))
            blob.upload_from_filename(os.path.join(ROOT_PATH, src_path, path))

    upload_file()
