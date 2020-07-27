import io
import pandas as pd

from google.cloud import secretmanager
from google.cloud import storage

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


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload
