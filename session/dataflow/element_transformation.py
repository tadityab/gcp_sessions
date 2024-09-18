

if __name__ == '__main__':
    import apache_beam
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
    import os

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    options = PipelineOptions()

    # specify google cloud option
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "bwt-learning-2024"
    google_cloud_options.job_name = "twooutputs"
    google_cloud_options.region = "asia-east1"
    google_cloud_options.staging_location = "gs://bwt-session-2024/stage_loc1/"
    google_cloud_options.temp_location = "gs://bwt-session-2024/temp_loc1"

    def printval(element):
        print("----------------")
        print(element)
        print("----------------")
        return element


    # # Map
    # with beam.Pipeline() as p:
    #     input = p | 'input' >> beam.Create([1,2,3,4,5])
    #     squre = input | 'square' >> beam.Map(lambda x: x*x)
    #     squre | 'print' >> beam.Map(print)

    # # FlatMap
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'input' >> beam.Create(['This is Apache beam', 'Running under Cloud Dataflow'])
    #             | 'flatmap' >> beam.FlatMap(lambda x: x.split(" "))
    #             | 'print' >> beam.Map(print)
    #
    #     )

    # # filter
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'input' >> beam.Create([1, 2, 3, 4, 5])
    #             | 'filterval' >> beam.Filter(lambda x: x > 3)
    #             | 'print' >> beam.Map(print)
    #     )

    # # ParDo
    # class TestParDo(beam.DoFn):
    #     def process(self, element):
    #         return element.split(" ")
    #
    #
    # with beam.Pipeline() as p:
    #     res = (
    #             p | 'input' >> beam.Create(['Hello how are you', 'I am good'])
    #             | 'pardoclass' >> beam.ParDo(TestParDo())
    #             | 'print' >> beam.Map(printval)
    #             | 'length' >> beam.Map(lambda x: (x, len(x)))
    #             | 'print1' >> beam.Map(print)
    #     )

    # # ParDo with Sideinput
    # class FilterClass(beam.DoFn):
    #     def process(self,element, side_input):
    #         if len(element) > side_input:
    #             yield element
    #
    # with beam.Pipeline() as p:
    #     side_input = 3
    #     res = (
    #         p | 'inpupt' >> beam.Create(['Ram','Sham','Apache','DataFlow'])
    #         | 'filter' >> beam.ParDo(FilterClass(),side_input)
    #         | 'print' >> beam.Map(print)
    #     )

    # ParDo with sideoutput
    class OddEven(beam.DoFn):
        def process(self, element):
            if element % 2 == 0:
                yield apache_beam.pvalue.TaggedOutput('even', element)
            else:
                yield apache_beam.pvalue.TaggedOutput('odd', element)


    with beam.Pipeline(options=options) as p:
        res = (
                p | 'input' >> apache_beam.Create([1, 2, 3, 4, 5, 6])
                | 'oddeven' >> apache_beam.ParDo(OddEven()).with_outputs('even', 'odd')
        )
        even_num = res.even
        odd_num = res.odd

        even_num | 'printeven' >> apache_beam.Map(printval)
        odd_num | 'printodd' >> apache_beam.Map(printval)

    # # Partition
    # def partitionoddeven(element, num_partitions):
    #     return 0 if element % 2 == 0 else 1
    #
    # with beam.Pipeline() as p:
    #     res = (
    #         p | 'input' >> beam.Create([1,2,3,4,5,6])
    #         | 'partition' >> beam.Partition(partitionoddeven,2)
    #     )
    #
    #     even = res[0]
    #     odd = res[1]
    #
    #     # even | 'printeven' >> beam.Map(print)
    #     odd | 'printodd' >> beam.Map(print)

    # Flatten
    # with beam.Pipeline() as p:
    #     pcol1 = p | 'createpcol1' >> beam.Create([1, 2, 3])
    #     pcol2 = p | 'createcol2' >> beam.Create([5, 6, 7])
    #
    #     flattern = (pcol1, pcol2) | 'Flattern' >> beam.Flatten() | 'print' >> beam.Map(print)

