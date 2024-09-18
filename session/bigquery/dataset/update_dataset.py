from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    dataset_id = f"{client.project}.bwt_session_2024"

    dataset = client.get_dataset(dataset_id)

    print(dataset)

    # description
    dataset.description = "This dataset is created using Cloud console"

    # friendlyname
    dataset.friendly_name = "BWT Session CC"

    # update
    updated_dataset = client.update_dataset(dataset,["description","friendly_name"])

    print(f"successfully updated dataset : {updated_dataset.dataset_id}, description: {updated_dataset.description}, friendly_name: {updated_dataset.friendly_name}")