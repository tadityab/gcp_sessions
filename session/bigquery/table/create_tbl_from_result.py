from google.cloud import bigquery
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    project_id = 'bwt-learning-2024'
    client = bigquery.Client(project=project_id)

    dest_tbl_id = f"{client.project}.bwt_session_cl.baseball_pcl"

    job_config = bigquery.QueryJobConfig(destination=dest_tbl_id)

    sql_query = """
      SELECT
        *
      FROM
        `bigquery-public-data.baseball.schedules` WHERE upper(homeTeamName) like 'M%'
    """

    query_res = client.query(sql_query, job_config=job_config)

    query_res.result()

    print(f"successfully copied data into table. {dest_tbl_id}")
