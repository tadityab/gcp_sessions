import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os, argparse


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_path')
        parser.add_value_provider_argument('--output_path')


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    # specify google cloud option
    user_option = options.view_as(UserOptions)

    with beam.Pipeline(options=options) as p:
        # Read input file
        read = p | 'Read' >> beam.io.ReadFromText(user_option.input_path)

        # convert to uppercase
        upper_case = read | 'upper case' >> beam.Map(lambda x: x.upper())

        #
        write = upper_case | 'Write data' >> beam.io.WriteToText(user_option.output_path)
