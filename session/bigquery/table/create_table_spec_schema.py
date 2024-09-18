from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)
    table_id = f"{client.project}.bwt_session_cl.employee_sepc_schema_pcl"

    # schema path
    schema_path = r"D:\GCP2024\employee_schema.json"

    schema = client.schema_from_json(schema_path)

    table = bigquery.Table(table_id,schema)

    res = client.create_table(table)

    print(f"table created : {table_id}")