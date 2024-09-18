import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"


    def printval(element):
        print("----------------------")
        print(element)
        print("----------------------")
        return element


    options = PipelineOptions()

    # # Map: A simpler version of ParDo that applies a function to each element in the PCollection.
    # # It is a shorthand for beam.ParDo.
    # with beam.Pipeline() as pipeline:
    #     squres = (
    #         pipeline | 'create' >> beam.Create([1,2,3,4,5])
    #         | 'squre' >> beam.Map(lambda x:x*x)
    #         | 'print' >> beam.Map(printval)
    #     )

    # # Filter: Applies a filter function to each element, emitting only those that pass a given condition.
    # with beam.Pipeline() as p:
    #     test = (
    #             p | 'create' >> beam.Create([1, 2, 3, 4, 5])
    #             | 'filter' >> beam.Filter(lambda x: x > 3)
    #             | 'print' >> beam.Map(printval)
    #     )

    # # FlatMap: Similar to Map, but the function can return multiple outputs for each input element, which are then flattened into a single PCollection.
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create(['This is test','Beam is cool'])
    #         | 'split' >> beam.FlatMap(lambda x: x.split(" "))
    #         | 'print' >> beam.Map(printval)
    #     )

    # # MapTuple: Similar to Map, but designed for PCollection elements that are tuples. It allows unpacking of tuple elements directly in the function, making it easier to work with key-value pairs.
    # with beam.Pipeline() as p:
    #     test = (
    #             p | 'create' >> beam.Create([('a', 1), ('b', 2), ('c', 3)])
    #             | 'increment' >> beam.MapTuple(lambda x, y: (x, y + 1))
    #             | 'print' >> beam.Map(print)
    #     )

    # # FlatMapTuple: Similar to FlatMap, but for elements that are tuples. It allows you to apply a function that returns an iterable to each element, and the results are flattened into a single PCollection.
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create([('a',[1,2]),('b',[3]),('c',[4,5])])
    #         | 'FlatMapTuple' >> beam.FlatMapTuple(lambda key, value:[(key, element) for element in value])
    #         | 'print' >> beam.Map(print)
    #     )

    # # ParDo: A powerful and flexible transformation that allows for more complex processing of elements using DoFn (Do Function) objects. ParDo can emit zero, one, or multiple output elements per input element, and is suitable for more sophisticated processing logic.
    # class SplitWordsFn(beam.DoFn):
    #     def process(self, element):
    #         return element.split(" ")
    #
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create(['Hello How are you','I am good'])
    #         | 'split' >> beam.ParDo(SplitWordsFn())
    #         | 'print' >> beam.Map(print)
    #     )

    # # ParDo with Side Inputs: Allows you to provide additional data to DoFn as "side inputs". Side inputs can be static values or entire PCollections that are used to influence the processing of the main PCollection.
    # class FilterWordsFn(beam.DoFn):
    #     def process(self, element, min_len):
    #         if len(element) >= min_length:
    #             yield element
    #
    # with beam.Pipeline() as p:
    #     input = p | 'create' >> beam.Create(['Ram Patil','Apache Beam', 'Python', 'test'])
    #     min_length = 5
    #     filter = input | 'filter' >> beam.ParDo(FilterWordsFn(), min_length)
    #     filter | beam.Map(print)

    # # ParDo with Side Outputs (MultiOutput): Allows a DoFn to produce multiple outputs, emitting each element to different output tags. This is useful when you want to separate data into multiple streams based on different conditions.
    #
    # class SplitFn(beam.DoFn):
    #     def process(self, element):
    #         if element.isalpha():
    #             yield beam.pvalue.TaggedOutput('words', element)
    #         else:
    #             yield beam.pvalue.TaggedOutput('number', element)
    #
    # with beam.Pipeline() as p:
    #     element = (
    #         p | 'create' >> beam.Create(['Ram','234','Sham','232'])
    #         | 'split' >> beam.ParDo(SplitFn()).with_outputs('words','number')
    #     )
    #
    #     words = element.words
    #     number = element.number
    #
    #     words | 'printwords' >> beam.Map(print)
    #     number | 'printnumber' >> beam.Map(print)

    # # Partition: Splits a PCollection into multiple PCollections based on a partitioning function. While it processes elements individually, it directs them into separate outputs.
    #
    # def partition_split(element, num_partitions):
    #     return 0 if element % 2 == 0 else 1
    #
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create([1,2,3,4,5,6])
    #         | 'partition' >> beam.Partition(partition_split,2)
    #     )
    #     even = test[0]
    #     odd = test[1]
    #
    #     even | 'printeven' >> beam.Map(print)
    #
    #     odd | 'printodd' >> beam.Map(print)

    # The Flatten transformation merges multiple PCollections of the same type into a single PCollection. This is useful when you have data split across multiple collections and want to consolidate it into one collection.
    with beam.Pipeline() as p:
        # Create two PCollections
        pcoll1 = p | 'Create PCollection 1' >> beam.Create([1, 2, 3, 4])
        pcoll2 = p | 'Create PCollection 2' >> beam.Create([5, 6, 7, 8])

        # Flatten them into a single PCollection
        flattened = (pcoll1, pcoll2) | 'Flatten' >> beam.Flatten()

        # Output the flattened PCollection
        flattened | 'PrintFlattened' >> beam.Map(print)