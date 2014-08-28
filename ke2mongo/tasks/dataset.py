#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

When output a dataset to CSV, can be loaded into a resource with: COPY \"{table}\"(\"{cols}\") FROM '{file}' DELIMITER ',' CSV ENCODING 'UTF8'

"""

import sys
import os
import luigi
import time
import numpy as np
import pandas as pd
import abc
import ckanapi
from monary import Monary
from ke2mongo.log import log
from ke2mongo.lib.timeit import timeit
from ke2mongo import config
from collections import OrderedDict
from pymongo import MongoClient
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.lib.file import get_export_file_dates
from ke2mongo.tasks.target import CSVTarget, CKANTarget

class DatasetTask(luigi.Task):
    """
    Class for processing data mongo into a dataset
    If date set, this task requires all mongo files for that date to have been imported into Mongo DB
    """
    # Date to process
    date = luigi.IntParameter(default=None)

    mongo_db = config.get('mongo', 'database')

    collection_name = 'ecatalogue'

    @abc.abstractproperty
    def columns(self):
        """
        Columns to use from mongoDB
        @return: list
        """
        return None

    @abc.abstractproperty
    def query(self):
        """
        Query object
        @return: list || dict
        """
        return None

    @abc.abstractmethod
    def output(self):
        """
        Output method
        This overrides luigi.task.output, to ensure it is set
        """
        return None

    def __init__(self, *args, **kwargs):

        # If a date parameter has been passed in, we'll just use that
        # Otherwise, loop through the files and get all dates
        super(DatasetTask, self).__init__(*args, **kwargs)

        export_file_dates = get_export_file_dates()
        # If we have more than one file export date, it could be problem if one of the mongo import files
        # So raise an exception, and ask the user to run manually
        if len(export_file_dates) > 1:
            raise IOError('There are multiple (%s) export files requiring processing. Please investigate and run bulk.py' % len(export_file_dates))

    def requires(self):
        # Call all mongo tasks to import latest mongo data dumps
        # If a file is missing, the process will terminate with an Exception
        # These run in reverse order, so MongoCatalogueTask runs last

        # Only require mongo tasks if data parameter is passed in - allows us to rerun for testing
        if self.date:
            yield MongoCatalogueTask(self.date), MongoDeleteTask(self.date), MongoTaxonomyTask(self.date),  MongoMultimediaTask(self.date), MongoMultimediaTask(self.date)

    @timeit
    def run(self):

        # Number of records to retrieve (~200 breaks CSV)
        block_size = 100 if isinstance(self.output(), CSVTarget) else 5000

        count = 0

        mongo = MongoClient()
        db = mongo[self.mongo_db]

        # Default collection name. This can be over-ridden by the $out setting in an aggregator
        collection_name = self.collection_name

        # Is this query object a list? (an aggregation query)
        if isinstance(self.query, list):

            # This is an aggregator, so needs building and the query will return everything ({})

            # The last element needs to be the out collection
            out = self.query[len(self.query) - 1]

            # Set the collection name to the out collection.
            # If last key isn't $out, this will raise an exception
            collection_name = out['$out']

            # Run the aggregation query
            log.info("Building aggregated collection: %s", collection_name)

            result = db[self.collection_name].aggregate(self.query, allowDiskUse=True)

            # Ensure the aggregation process succeeded
            assert result['ok'] == 1.0

            # Select everything from the aggregation pipeline
            query = {}

        elif isinstance(self.query, dict): # A normal mongo query
            query = self.query
        else:
            raise TypeError('Query needs to be either an aggregation list or query dict')

        with Monary() as m:

            query_fields, df_cols, field_types, indexed = zip(*self.columns)

            catalogue_blocks = m.block_query(self.mongo_db, collection_name, query, query_fields, field_types, block_size=block_size)

            for catalogue_block in catalogue_blocks:

                # Columns are indexed by key in the catalogue
                catalogue_block = [arr.astype(np.str).filled('') if self._is_output_field(df_cols[i]) else arr for i, arr in enumerate(catalogue_block)]

                # Create a pandas data frame with block of records
                # Columns use the name from the output columns - but must be in the same order as query_fields
                # Which is why we're using tuples for the columns
                df = pd.DataFrame(np.matrix(catalogue_block).transpose(), columns=df_cols)

                # Loop through all the columns and ensure hidden integer fields are cast as int32
                # For example, taxonomy_irn is used to join with taxonomy df
                for i, df_col in enumerate(df_cols):
                    if not self._is_output_field(df_col) and field_types[i] == 'int32':
                        df[df_col] = df[df_col].astype('int32')

                df = self.process_dataframe(m, df)

                # Output the dataframe
                self.output().write(df)

                row_count, col_count = df.shape
                count += row_count
                log.info("\t %s records", count)

    def process_dataframe(self, m, df):
        return df  # default impl

    @staticmethod
    def _is_output_field(field):
        """
        Fields starting with _ are hidden and shouldn't be included in output (excluding _id)
        @param field:
        @return: bool
        """
        return field == '_id' or not field.startswith('_')

    def get_output_columns(self):
        return OrderedDict((col[1], col[2]) for col in self.columns if self._is_output_field(col[1]))


class DatasetToCKANTask(DatasetTask):
    """
    Output dataset to CKAN
    """
    @abc.abstractproperty
    def package(self):
        """
        Package property
        @return: dict
        """
        return None

    @abc.abstractproperty
    def datastore(self):
        """
        Datastore property
        @return: dict
        """
        return None

    @property
    def primary_key(self):
        """
        Optional primary key property
        @return: str
        """
        return None

    def __init__(self, *args, **kwargs):

        super(DatasetToCKANTask, self).__init__(*args, **kwargs)

        # Get resource id - and create datastore if it doesn't exist
        # Set up connection to CKAN
        self.ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

        try:
            # If the package exists, retrieve the resource
            package = self.ckan.action.package_show(id=self.package['name'])
            self.resource_id = package['resources'][0]['id']

        except ckanapi.NotFound:

            log.info("Package %s not found - creating", self.package['name'])

            # Create the package
            package = self.ckan.action.package_create(**self.package)
            # And create the datastore
            self.datastore['resource']['package_id'] = package['id']
            # Add the field indexes
            # Add which fields should be indexed
            indexes = [col[1] for col in self.columns if col[3] and col[2].startswith('string')]
            fields = [{'id': col[1], 'type': 'text'} for col in self.columns if col[3]]

            self.datastore['indexes'] = indexes
            self.datastore['fields'] = fields

            # API call to create the datastore
            datastore = self.ckan.action.datastore_create(**self.datastore)

            self.resource_id = datastore['resource_id']

            # If this has geospatial fields, create geom columns
            if self.geospatial_fields:
                log.info("Creating geometry columns for %s", self.resource_id)
                self.geospatial_fields['resource_id'] = self.resource_id
                self.ckan.action.create_geom_columns(**self.geospatial_fields)

            log.info("Created datastore resource %s", self.resource_id)

    def output(self):
        return CKANTarget(self.resource_id)


class DatasetToCSVTask(DatasetTask):
    """
    Output dataset to CSV
    """

    @property
    def file_name(self):
        """
        File name to output
        @return: str
        """
        return self.__class__.__name__.replace('DatasetToCSVTask', '').lower()

    def output(self):
        """
        Luigi method: output target
        @return: luigi file ref
        """
        return CSVTarget(file_name=self.file_name, date=self.date, columns=self.get_output_columns())