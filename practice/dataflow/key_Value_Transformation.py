import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\GCP2024\gcp_sessions\bwt-learning-2024-cf237a407937.json"

    lst = [('a',1),('b',2),('c',3)]
    def printval(element):
        print("----------------------")
        print(element)
        print("----------------------")
        return element


    options = PipelineOptions()

    # # GroupByKey: Groups elements in a PCollection by a key. It requires the input PCollection to be of type (key, value).
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create([('a',1),('b',2),('a',3)])
    #         | 'group' >> beam.GroupByKey()
    #         | 'print' >> beam.Map(printval)
    #     )

    # # CoGroupByKey: Performs a cogroup (similar to a SQL join) on two or more PCollections based on a common key.
    # with beam.Pipeline() as p:
    #     student = p | 'createstudent' >> beam.Create([('name','Ram'),('city','Pune')])
    #     teacher = p | 'createteacher' >> beam.Create([('name','Ram'), ('country', 'India')])
    #
    #     result = (
    #         {'student': student, 'teacher':teacher}
    #         | 'cogroup' >> beam.CoGroupByKey()
    #         | 'printval1' >> beam.Map(print)
    #     )

    # # CombinePerKey: Applies a specified combining function to all the values associated with a key. This is useful for aggregations like sums, averages, counts, etc., on a per-key basis.
    #
    # with beam.Pipeline() as p:
    #     test = (
    #             p | 'create' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
    #             | 'sumperkey' >> beam.CombinePerKey(sum)
    #             | 'print' >> beam.Map(print)
    #     )

    # # CountPerKey: A specialized CombinePerKey transformation that counts the occurrences of each key.
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create([('a',1),('b',2),('a',3)])
    #         | 'countperkey' >> beam.combiners.Count.PerKey()
    #         | 'print' >> beam.Map(print)
    #     )

    # # TopPerKey: Retrieves the top N elements for each key based on a specified comparison function.
    # with beam.Pipeline() as p:
    #     test = (
    #             p | 'create' >> beam.Create([('a', 8), ('b', 2), ('a', 3), ('b', 5)])
    #             | 'topperkey' >> beam.combiners.Top.PerKey(2)
    #             | 'print' >> beam.Map(print)
    #     )

    # # FilterByKey: Filters a PCollection of key-value pairs based on a condition applied to the key.
    # with beam.Pipeline() as p:
    #     test = (
    #             p | 'create' >> beam.Create([('a', 1), ('b', 3), ('c', 3)])
    #             | 'filter' >> beam.Filter(lambda t:t[0] == 'a')
    #             | 'print' >> beam.Map(print)
    #     )

    # # MapValues: Transforms the values of a PCollection of key-value pairs while keeping the keys unchanged. This is useful for applying a function to the values while preserving the key structure.
    # with beam.Pipeline() as p:
    #     test = (
    #         p | 'create' >> beam.Create(lst)
    #         | 'mapvalue' >> beam.Map(lambda t:(t[0],t[1]*2))
    #         | 'print' >> beam.Map(print)
    #     )

    # # Keys: Extracts the keys from a PCollection of key-value pairs, discarding the values.
    # # Values: Extracts the values from a PCollection of key-value pairs, discarding the keys.
    # with beam.Pipeline() as p:
    #     # Example PCollection of key-value pairs
    #     kv_pairs = p | 'CreatePairs' >> beam.Create([('a', 1), ('b', 2), ('c', 3)])
    #
    #     # Extract the keys using beam.Keys()
    #     keys = kv_pairs | 'ExtractKeys' >> beam.Keys()
    #
    #     # values
    #     values = kv_pairs | 'value' >> beam.Values()
    #
    #     # Print the keys to the console (for testing purposes)
    #     keys | 'PrintKeys' >> beam.Map(print)
    #     values | 'print value' >> beam.Map(print)

    # ReducePerKey: Applies a binary operator to reduce the values associated with each key in a PCollection of key-value pairs to a single output value per key.
    with beam.Pipeline() as p:
        key_value_pairs = p | 'Create' >> beam.Create([('a', 1), ('a', 2), ('b', 3), ('b', 4)])

        reduced = key_value_pairs | 'ReducePerKey' >> beam.CombinePerKey(lambda a, b: a + b)

        reduced | beam.Map(print)  # Output: ('a', 3), ('b', 7)

