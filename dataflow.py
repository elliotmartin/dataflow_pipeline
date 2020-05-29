from __future__ import absolute_import

import argparse
import logging
import re
import ast


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

PROJECT = "CLOUD_PROJECT"
TOPIC = "PUBSUB_TOPIC"
DATASET = "BIGQUERY_DATASET"
TABLE = "BIGQUERY_TABLE"

class Decode(beam.DoFn):
    def process(self, element):
        res = element.decode('utf-8')
        yield res

class Format(beam.DoFn):
    def process(self, element):
        element = ast.literal_eval(element)
        logging.info('tuple(element) = ' + str(element))
        formatted = {'Cube' : element[0], 'DateTime' : element[1]}
        yield formatted

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default='projects/' + PROJECT + '/topics/' + TOPIC, help='PubSub topic to listen on')
    parser.add_argument('--output', dest='output', default= DATASET + '.' + TABLE, help='BQ table to write to') 
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    topic = p | 'listen on PubSub' >> beam.io.ReadFromPubSub(topic=known_args.input)

    formatted = (topic
            | 'decode' >> beam.ParDo(Decode())
            | 'format' >> beam.ParDo(Format()))

    output = formatted | 'Write to BQ' >> beam.io.WriteToBigQuery(known_args.output,schema='Cube:STRING,DateTime:DATETIME', write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result=p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

