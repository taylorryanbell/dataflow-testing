import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


INPUT = "gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/part-r-*"
OUTPUT = "gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/results/output"


class StripHeader(beam.DoFn):
    def process(self, element):

        # check to see if the given line (element) is the header, and if so, skip it, otherwise yield the line
        if str(element) == 'author,points':
            pass
        else:
            yield element


def run(argv=None, save_main_session=True):


    # The pipeline will be run on exiting the with block.
    with beam.Pipeline() as pipeline:

        # read all of the files with a given name prefix from the specified storage bucket.
        # print(known_args.input)
        # print(type(known_args.input))
        lines = pipeline | 'Read' >> beam.io.ReadFromText(INPUT)

        # using a homemade PTransform to remove the Header in each file
        out_col = lines | beam.ParDo(StripHeader())

        # write a new file and include a single header at the top.
        out_col | beam.io.WriteToText(OUTPUT, file_name_suffix='.csv', header="author,points")



if __name__ == '__main__':

    print('+-------------------------+')
    print('| GCP CSV Congregate v1.3 |')
    print('+-------------------------+')
    logging.getLogger().setLevel(logging.INFO)
    run()