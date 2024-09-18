import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    # specify google cloud option
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "bwt-learning-2024"
    google_cloud_options.job_name = "twooutputs"
    google_cloud_options.region = "us-east1"
    google_cloud_options.staging_location = "gs://bwt-dataproc-stg-bucket/stage_loc1/"
    google_cloud_options.temp_location = "gs://bwt-dataproc-stg-bucket/temp_loc1"


    def printval(element):
        logging.info("-----------------")
        logging.info(f"element: {element}")
        logging.info(f"element type : {type(element)}")
        logging.info("-----------------")
        return element


    class ConvertDict(beam.DoFn):
        def process(self, element):
            element['difference'] = element['total_marks'] - element['marks']
            yield element


    with beam.Pipeline(options=options) as p:
        query = """
        SELECT * FROM `bwt-learning-2024.bwt_session_cl.student_details_tbl` where marks >= 50
        """
        res = (
                p | 'read' >> beam.io.gcp.bigquery.ReadFromBigQuery(query=query, use_standard_sql=True)
                | 'select' >> beam.ParDo(ConvertDict())
                | 'print' >> beam.Map(printval)
        )
        #
        # res = (
        #     p | 'read' >> beam.io.gcp.bigquery.ReadFromBigQuery(table='bwt-learning-2024.bwt_session_cl.student_details_tbl')
        #     | 'select' >> beam.Map(lambda x: (x['id'],x['name']))
        #     | 'print' >> beam.Map(printval)
        # )

        # write into bigquery table
        # write = res | 'writetobq' >> beam.io.gcp.bigquery.WriteToBigQuery(
        #     table="bwt-learning-2024.bwt_session_cl.student_details1",
        #     schema="id:integer,name:string",
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        # )

