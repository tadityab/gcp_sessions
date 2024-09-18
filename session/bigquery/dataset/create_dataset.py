from google.cloud import bigquery
import os
if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    print(f"currently working in project: {client.project}")

    dataset_id = f"{client.project}.bwt_session_cl"

    dataset = bigquery.Dataset(dataset_ref=dataset_id)

    dataset.location = "US"
    dataset.description = "This dataset is created using python client lib"
    dataset.friendly_name = "BWT Sesssion"
    dataset.description = "This dataset is created using python client lib."

    dataset = client.create_dataset(dataset)

    print(f"successfully create dataset : {client.project}.{dataset.dataset_id}")