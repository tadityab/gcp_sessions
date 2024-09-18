from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    datasets = list(client.list_datasets())

    for dataset in datasets:
        print(f"dataset name: {dataset.dataset_id} | friendly name: {dataset}")

        dataset_det = client.get_dataset(dataset.dataset_id)
        print(f"friendly name: {dataset_det.friendly_name}")

