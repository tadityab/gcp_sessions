import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

if __name__ == '__main__':
    options = PipelineOptions()

    lst = ['ram patil', 'sham patil', 'manoj kadam', 'amit kurde']

    # 1. convert each element into camel case
    with beam.Pipeline() as p:
        input = (
                p | 'input' >> beam.Create(lst)
        )
        # res = (
        #         input
        #         | 'camel case' >> beam.Map(lambda x: x.title())
        #         | 'print' >> beam.Map(print)
        # )
        #
        # res = (
        #     input | 'countchar' >> beam.Map(lambda x:(x,len(x)))
        #     | 'print' >> beam.Map(print)
        # )
        res = (
            input | 'split' >> beam.FlatMap(lambda x:x.split(" "))
            | 'countchar' >> beam.Map(lambda x:(x,len(x)))
        )

        class OddEven(beam.DoFn):
            def process(self, element):
                if element[1] % 2 == 0:
                    yield beam.pvalue.TaggedOutput('even',element)
                else:
                    yield beam.pvalue.TaggedOutput('odd', element)

        third_res = (
            res | 'oddeven' >> beam.ParDo(OddEven()).with_outputs('even','odd')
        )

        # even = third_res.even
        #
        # even | 'even' >> beam.Map(print)
        #
        odd = third_res.odd

        odd | 'even' >> beam.Map(print)

    # 2. character count

    # 3. retrieve even and odd names -
