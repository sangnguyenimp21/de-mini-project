import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
from apache_beam.io import WriteToText


PROJECT_ID = 'tempproject-9380d'
SCHEMA = 'id:INTEGER,name:STRING,city:STRING,state:STRING'

def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['id']) > 0 and len(data['name']) > 0 and len(data['city']) > 0 and len(data['state']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['city'] = str(data['city']) if 'city' in data else None
    data['state'] = str(data['state']) if 'state' in data else None
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://group6-miniproject/breweries.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "city": x[2], "state": x[3]})
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:beer.brewery_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()