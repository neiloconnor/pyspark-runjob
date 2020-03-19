# Imports the Google Cloud client library
from google.cloud import storage
import job_settings

def create_bucket(bucket_name):
    # Instantiates a client
    storage_client = storage.Client()

    # Creates the new bucket
    bucket = storage_client.create_bucket(bucket_name)

    print(f'Bucket {bucket.name} created')

def upload_file(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    # Uploaded file will have the same filename
    blob = bucket.blob(file_name)

    # Upload file
    blob.upload_from_filename(file_name)

    print(f'File {file_name} uploaded to {bucket_name}')

if __name__ == "__main__":
    create_bucket(job_settings.BUCKET_NAME)
    upload_file(job_settings.BUCKET_NAME, job_settings.DATA_FILENAME)
    upload_file(job_settings.BUCKET_NAME, job_settings.CODE_FILENAME)