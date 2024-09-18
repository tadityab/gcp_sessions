from google.cloud import storage
import os

if __name__ == '__main__':
    # initialization of google application
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-ddcf2bbc7778.json"

    client = storage.Client(project="bwt-learning-2024")

    print(client.project)

    # create bucket
    bucket_name = "bwt-session-python-client"

    bucket = client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = client.create_bucket(bucket,location="asia-east1")

    print(f"new bucket is created : {new_bucket.name}")

    # list all the buckets
    buckets = client.list_buckets()

    for item in buckets:
        print(f'bucket name: {item.name}')


    # delete  bucket
    bucket = client.get_bucket(bucket_name)
    bucket.delete()
    print(f'deleted bucket : {bucket.name}')

    # list all the buckets
    buckets = client.list_buckets()

    for item in buckets:
        print(f'bucket name: {item.name}')