import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
from apache_beam.transforms.window import FixedWindows

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    with beam.Pipeline() as p:
        input = p | 'create' >> beam.Create([
            ('a1', '2024-09-01T00:01:30'),
            ('a2', '2024-09-01T00:02:30'),
            ('a3', '2024-09-01T00:03:30')
        ])
        print1 = input | 'fixedwindow' >> beam.WindowInto(FixedWindows(60)) | 'print' >> beam.Map(print)
