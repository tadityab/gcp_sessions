import apache_beam as beam
import apache_beam.dataframe.convert
from apache_beam.dataframe.convert import to_dataframe, to_pcollection
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    # specify google cloud option
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "bwt-learning-2024"
    google_cloud_options.job_name = "simpledataflowjob"
    google_cloud_options.region = "asia-east1"
    google_cloud_options.staging_location = "gs://bwt-session-2024/stage_loc1/"
    google_cloud_options.temp_location = "gs://bwt-session-2024/temp_loc1"

    with beam.Pipeline(options=options) as p:
        read = p | 'read' >> beam.io.ReadFromCsv(r"D:\GCP2024\gcp_sessions\session\inputdata\employee.csv",
                                                 sep=',',
                                                 header=0)
        # dataframe
        convert = to_dataframe(read)

        # select columns
        select_df = convert[["id", "name"]]

        # increase id
        select_df['increased_id'] = select_df['id'] + 1

        #
        printval = to_pcollection(select_df)

        printval | 'print' >> beam.Map(print)
