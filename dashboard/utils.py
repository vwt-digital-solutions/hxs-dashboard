import io
import base64
import pandas as pd

from google.cloud import storage
from google.cloud import kms_v1

client = storage.Client()


def download_as_string(bucket_name, blob_name):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_string()
    return data


def download_as_buffer(bucket_name, blob_name):
    data = download_as_string(bucket_name, blob_name)
    buffer = io.BytesIO()
    buffer.write(data)
    return buffer


def download_as_dataframe(bucket_name, blob_name, sheet_name=0):
    buffer = download_as_buffer(bucket_name, blob_name)
    df = pd.read_excel(buffer, sheet_name)
    buffer.close()
    return df


def decrypt_secret(project, region, keyring, key, secret_base64):
    secret_enc = base64.b64decode(secret_base64)
    kms_client = kms_v1.KeyManagementServiceClient()
    key_path = kms_client.crypto_key_path_path(project, region, keyring, key)
    secret = kms_client.decrypt(key_path, secret_enc)
    return secret.plaintext.decode("utf-8").replace('\n', '')
