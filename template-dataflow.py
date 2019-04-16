from __future__ import absolute_import

import csv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions


class UploadOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            default='gs://some/input.csv',
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--output',
            required=True,
            help='Output file to write results to.')

pipeline_options = PipelineOptions(['--output', 'gs://some/output'])
p = beam.Pipeline(options=pipeline_options)
upload_options = pipeline_options.view_as(UploadOptions)

(p
    | 'read' >> beam.io.Read(upload_options.input)
    | 'Write' >> beam.io.WriteToText(upload_options.output, file_name_suffix='.csv'))
