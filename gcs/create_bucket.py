import os
from google.cloud import storage

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"
    client = storage.Client(project='bwt-learning-2024')

    print(client.project)

    buckets = client.list_buckets()
    for item in buckets:
        print(item.name)
    # bucket_name = "bwt-python-test"
    #
    # storage_client = storage.Client()
    #
    # bucket = storage_client.bucket(bucket_name)
    # bucket.storage_class = "COLDLINE"
    # new_bucket = storage_client.create_bucket(bucket, location="asia-east1")
    #
    # print(
    #     "Created bucket {} in {} with storage class {}".format(
    #         new_bucket.name, new_bucket.location, new_bucket.storage_class
    #     )
    # )
    # print(new_bucket)
