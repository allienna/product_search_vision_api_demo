import json
import logging
import re
from typing import Tuple

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from httplib2 import Http


class TransformToProductSetsDoFn(beam.DoFn):
    """Transform catalog information to Product Set"""

    def __init__(self, image_path, product_set_id):
        self.image_path = image_path
        self.product_set_id = product_set_id

    def process(self, element, **kwargs):
        """Returns a product set as dictionary.

        The element is a PCollection of one element (a dictionary) that represent a product.

        Args:
            element: the element to be processed.
            **kwargs: other keyword arguments.
        """
        # id, gender, masterCategory, subCategory, articleType, baseColour, season, year, usage, productDisplayName
        product_id, gender, _, sub_category, article_type, _, _, _, _, product_display_name = element.split(',')

        d = {
            'image-uri': f'{self.image_path}{product_id}.jpg',
            'image-id': product_id,
            'product-set-id': self.product_set_id.get(),
            'product-id': product_id,
            'product-category': 'apparel-v2',
            'product-display-name': product_display_name,
            'labels': f'"gender={gender},subCategory={sub_category},articleType={article_type}"',
            'bounding-poly': ''
        }

        yield d


class TransformCatalogToProductSetsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--catalog_definition_path",
            help=('The path of catalog definition file'
                  'gs://<bucket_name>/<location_path>')
        )

        parser.add_value_provider_argument(
            "--product_set_images_path",
            help=('The path of images directory'
                  '"gs://<bucket_name>/<location_path>"')
        )

        parser.add_value_provider_argument(
            "--product_set_id",
            help='The ID for the product set you want to run the operation on',
            default='product_catalog'
        )

        parser.add_value_provider_argument(
            "--product_set_output_path",
            help=('Result pipeline location, where product sets are stored'
                  '"gs://<bucket_name>/<location_path>"')
        )


def run(argv=None):
    """Main entry point; defines and run the pipeline."""

    # Parse argument from the command line.
    options = PipelineOptions(flags=argv)

    with Pipeline(options=options) as p:
        # Initiate the pipeline using the pipeline arguments passed in from the command line.
        # This includes information such as the project ID and where Dataflow should store temp files
        pipeline_options = options.view_as(TransformCatalogToProductSetsOptions)

        (p
         | 'QueryTable' >> beam.io.ReadFromText(
                    file_pattern=pipeline_options.catalog_definition_path,
                    skip_header_lines=1)
         | 'Transform data to product sets' >> beam.ParDo(TransformToProductSetsDoFn(
                    image_path=pipeline_options.product_set_images_path,
                    product_set_id=pipeline_options.product_set_id))
         | 'CSV format' >> beam.Map(lambda row: ','.join([str(row[column]) for column in row]))
         | 'Store product sets to GCS' >> beam.io.WriteToText(
                    file_path_prefix=pipeline_options.product_set_output_path,
                    shard_name_template='',
                    num_shards=1,
                    file_name_suffix='.csv',
                    header='image-uri, image-id, product-set-id, product-id, product-category, product-display-name, labels, bounding-poly'
                )
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
