import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os

# The most general parallel processing transformation.
# It applies a user-defined function (a DoFn) to each element in the PCollection.
# This is similar to a map function in other frameworks.

class SplitWords(beam.DoFn):
    def process(self, element):
        return element.split()


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    def printval(element):
        print("---------------")
        print(element)
        print("---------------")
        return element

    with beam.Pipeline() as pipeline:
        words = (
                pipeline
                | 'ReadLines' >> beam.Create(['This is a test', 'Beam is cool'])
                | 'Map element1' >> beam.Map(printval)
                | 'SplitWords' >> beam.ParDo(SplitWords())
                | 'Map element' >> beam.Map(printval)
        )
