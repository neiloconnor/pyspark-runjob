# Imports the Google Cloud client library
from google.cloud import storage
from google.api_core.exceptions import NotFound
from google.cloud import dataproc_v1 as dataproc
import time

import settings

def create_bucket(bucket_name):
    # Instantiates a client
    storage_client = storage.Client()

    # Check if the bucket exists
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f'Bucket {bucket.name} exists')

    except NotFound:
        # Create the new bucket
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


def run_dataproc_job(project_id, region, cluster_name, job_file_path):

    print('Creating cluster ...')
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    })

    # Create the cluster config.
    cluster = {
        'project_id': project_id,
        'cluster_name': cluster_name,
        'config': {
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-1'
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-1'
            }
        }
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(project_id, region, cluster)
    result = operation.result()

    print('Cluster created successfully: {}'.format(result.cluster_name))


    print('Submitting job ...')
    # Create the job client.
    job_client = dataproc.JobControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    })

    # Create the job config.
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': job_file_path
        }
    }

    job_response = job_client.submit_job(project_id, region, job)
    job_id = job_response.reference.job_id

    print('Submitted job \"{}\".'.format(job_id))

    # Termimal states for a job.
    terminal_states = {
        dataproc.types.JobStatus.ERROR,
        dataproc.types.JobStatus.CANCELLED,
        dataproc.types.JobStatus.DONE
    }

    # Create a timeout such that the job gets cancelled if not in a
    # terminal state after a fixed period of time.
    timeout_seconds = 600
    time_start = time.time()

    # Wait for the job to complete.
    while job_response.status.state not in terminal_states:
        if time.time() > time_start + timeout_seconds:
            job_client.cancel_job(project_id, region, job_id)
            print('Job {} timed out after threshold of {} seconds.'.format(
                job_id, timeout_seconds))

        # Poll for job termination once a second.
        time.sleep(1)
        job_response = job_client.get_job(project_id, region, job_id)

    # Cloud Dataproc job output gets saved to a GCS bucket allocated to it.
    cluster_info = cluster_client.get_cluster(
        project_id, region, cluster_name)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(cluster_info.config.config_bucket)
    output_blob = (
        'google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'
        .format(cluster_info.cluster_uuid, job_id))
    output = bucket.blob(output_blob).download_as_string()

    print('Job {} finished with state {}:\n{}'.format(
        job_id,
        job_response.status.State.Name(job_response.status.state),
        output))

    print('Deleting cluster ...')
    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(project_id, region, cluster_name)
    operation.result()

    print('Cluster {} successfully deleted.'.format(cluster_name))

if __name__ == "__main__":
    create_bucket(settings.BUCKET_NAME)
    # upload_file(settings.BUCKET_NAME, settings.DATA_FILENAME)
    # upload_file(settings.BUCKET_NAME, settings.CODE_FILENAME)

    # run_dataproc_job(settings.PROJECT_ID, settings.REGION, settings.CLUSTER_NAME, f'gs://{settings.BUCKET_NAME}/{settings.CODE_FILENAME}')