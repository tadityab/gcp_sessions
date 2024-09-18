from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    view_id = f"{client.project}.bwt_session_cl.employee_view_pcl"

    view = bigquery.Table(view_id)

    view.view_query = "select * from `bwt-learning-2024.bwt_session_cl.session_employee_ui`"

    view = client.update_table(view, ["view_query"])
    print(f"updated {view.reference}")