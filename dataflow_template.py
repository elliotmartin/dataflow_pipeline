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

class Decode(beam.DoFn):
    def process(self, element):
        res = element.decode('utf-8')
        yield res

class Format(beam.DoFn):
    def process(self, element):
        element = ast.literal_eval(element)
        #print(element[0])
        formatted = {'Cube' : element[0], 'DateTime' : element[1]}
        yield formatted

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', dest='input', default='projects/MYPROJECT/topics/MYTOPIC', help='PubSub topic to listen on')
        parser.add_argument('--output', dest='output', default='DATASET.TABLE', help='BQ table to write to')

def run(argv=None, save_main_session=True):

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)
    
    my_options = pipeline_options.view_as(MyOptions)

    topic = p | 'listen on PubSub' >> beam.io.ReadFromPubSub(topic=my_options.input)

    formatted = (topic
            | 'decode' >> beam.ParDo(Decode())
            | 'format' >> beam.ParDo(Format()))

    output = formatted | 'Write to BQ' >> beam.io.WriteToBigQuery(my_options.output,schema='Cube:STRING,DateTime:DATETIME', write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result=p.run()
    result.wait_until_finish()

logging.getLogger().setLevel(logging.INFO)
run()

