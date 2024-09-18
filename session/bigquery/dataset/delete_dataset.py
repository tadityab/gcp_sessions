from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    dataset = client.dataset("bwt_session_cl", client.project)

    client.delete_dataset(dataset,
                          delete_contents=True, not_found_ok=True)

    print(f"Dataset is deleted : bwt-learning-2024.bwt_session_cl")