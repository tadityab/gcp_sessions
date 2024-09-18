import apache_beam as beam
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
    google_cloud_options.staging_location = "gs://bwt-session-2024/stage_loc/"
    google_cloud_options.temp_location = "gs://bwt-session-2024/temp_loc"

    input_file = r"gs://bwt-session-2024/dataflow/input/abc.txt"
    output_file = r"gs://bwt-session-2024/dataflow/output/simpledata"

    with beam.Pipeline(options=options) as p:
        # Read input file
        read = p | 'Read' >> beam.io.ReadFromText(input_file)

        # convert to uppercase
        upper_case = read | 'upper case' >> beam.Map(lambda x:x.upper())

        #
        write = upper_case | 'Write data' >> beam.io.WriteToText(output_file)

