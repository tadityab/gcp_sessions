from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)
    table_id = f"{client.project}.bwt_session_cl.employee_pcl"

    schema = [
        bigquery.SchemaField("id", "Integer", mode="Required", description="contains employee id"),
        bigquery.SchemaField("name", "string")
    ]

    table = bigquery.Table(table_id, schema)

    table = client.create_table(table)

    print(f"successfully created table : {table.table_id} under dataset : {table.dataset_id}")
