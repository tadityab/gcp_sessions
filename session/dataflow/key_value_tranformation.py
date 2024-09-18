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
    google_cloud_options.staging_location = "gs://bwt-session-2024/stage_loc1/"
    google_cloud_options.temp_location = "gs://bwt-session-2024/temp_loc1"

    # # GroupByKey
    # with beam.Pipeline() as p:
    #     res = (
    #         p | 'input' >> beam.Create([('a',1),('b',2),('a',3)])
    #         | 'groupbykey' >> beam.GroupByKey()
    #         | 'print' >> beam.Map(print)
    #     )

    # # CoGroupByKey
    # with beam.Pipeline(options=options) as p:
    #     pcol1 = p | 'pcol1' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
    #     pcol2 = p | 'pcol2' >> beam.Create([('a', 3), ('b', 4), ('d', 3)])
    #     res = (
    #             {'pcol1': pcol1, 'pcol2': pcol2}
    #             | 'cogroup' >> beam.CoGroupByKey()
    #             | 'printval' >> beam.Map(print)
    #     )

    # # CombinePerKey
    # with beam.Pipeline() as p:
    #     res = (
    #         p | 'input' >> beam.Create([('a',1),('b',2),('a',3)])
    #         | 'sumperkey' >> beam.CombinePerKey(sum)
    #         | 'print' >> beam.Map(print)
    #     )

    # # CountPerKey
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'input' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
    #             | 'countperkey' >> beam.combiners.Count.PerKey()
    #             | 'print' >> beam.Map(print)
    #     )

    # # TopPerKey
    # with beam.Pipeline() as p:
    #     res = (
    #         p | 'input' >> beam.Create([('a',1),('b',2),('a',3)])
    #         | 'topperkey' >> beam.combiners.Top.PerKey(1)
    #         | 'print' >> beam.Map(print)
    #     )

    # # filter on key or value
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'create' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
    #         | 'filterkey' >> beam.Filter(lambda x:x[0] == 'a')
    #             | 'print' >> beam.Map(print)
    #     )

    # # key or value
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'input' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
    #             | 'valueoper' >> beam.Map(lambda t:(t[0],t[1]*t[1]))
    #             | 'print' >> beam.Map(print)
    #     )

    # # key values extract
    # with beam.Pipeline() as p:
    #     res = (
    #         p | 'input' >> beam.Create([('a',1),('b',2),('a',3)])
    #     )
    #     keys = res | 'extractkey' >> beam.Keys()
    #     values = res | 'extractvalues' >> beam.Values()
    #
    #     keys | 'print' >> beam.Map(print)