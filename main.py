import csv
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText

class stripHeader(beam.DoFn):
    def process(self, element):

        # check to see if the given line (element) is the header, and if so, skip it, otherwise yield the line
        if str(element) == 'author,points':
            pass
        else:
            yield element


if __name__ == '__main__':

    print('+-------------------------+')
    print('| GCP CSV Congregate v1.3 |')
    print('+-------------------------+')

    with beam.Pipeline() as pipeline:

        # read all of the files with a given name prefix from the specified storage bucket.
        lines = pipeline | 'Read' >> beam.io.ReadFromText('gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/part-r-*')

        # using a homemade PTransform to remove the Header in each file
        out_col = lines | beam.ParDo(stripHeader())

        # write a new file and include a single header at the top.
        out_col | beam.io.WriteToText('gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/author_rankings', file_name_suffix='.csv', header="author,points")