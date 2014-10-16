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
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.delete import DeleteTask
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

    @abc.abstractmethod
    def output(self):
        """
        Output method
        This overrides luigi.task.output, to ensure it is set
        """
        return None

    @property
    def query(self):
        """
        Query object
        @return: dict
        """

        query = {"ColRecordType": self.record_type}

        if self.date:
            query['exportFileDate'] = self.date

        return query

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
            yield MongoCatalogueTask(self.date), DeleteTask(self.date), MongoTaxonomyTask(self.date),  MongoMultimediaTask(self.date), MongoMultimediaTask(self.date)

    @timeit
    def run(self):

        # # Default collection name - overridden by aggregation queries
        # collection_name = self.collection_name

        # Number of records to retrieve (~200 breaks CSV)
        block_size = 100 if isinstance(self.output(), CSVTarget) else 5000
        count = 0

        with Monary() as m:

            log.info("Querying Monary")

            query_fields, df_cols, field_types = zip(*self.columns)

            catalogue_blocks = m.block_query(self.mongo_db, self.collection_name, self.query, query_fields, field_types, block_size=block_size)

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

    def get_dataframe(self, m, collection, columns, irns, key):

        query_fields, df_cols, field_types = zip(*columns)
        assert key in df_cols, 'Merge dataframe key must be present in dataframe columns'

        q = {'_id': {'$in': irns}}

        query = m.query('keemu', collection, q, query_fields, field_types)
        df = pd.DataFrame(np.matrix(query).transpose(), columns=df_cols)

        # Convert to int
        df[key] = df[key].astype('int32')
        # And make index
        df.index = df[key]

        return df

    @staticmethod
    def _is_output_field(field):
        """
        Fields starting with _ are hidden and shouldn't be included in output
        @param field:
        @return: bool
        """
        return not field.startswith('_')

    def get_output_columns(self):
        return OrderedDict((col[1], col[2]) for col in self.columns if self._is_output_field(col[1]))


class DatasetToCKANTask(DatasetTask):
    """
    Output dataset to CKAN
    """
    geospatial_fields = None

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

    def output(self):
        return CKANTarget(package=self.package, datastore=self.datastore, columns=self.get_output_columns(), geospatial_fields=self.geospatial_fields)


# If this works, all tasks will be CKAN tasks.
# So move dataset creation to DatasetTask

class DatasetToCSVTask(DatasetTask):
    """
    Output dataset to CSV
    """
    @property
    def path(self):
        """
        File name to output
        @return: str
        """
        file_name = self.__class__.__name__.replace('DatasetToCSVTask', '').lower()

        if self.date:
            file_name += '-' + str(self.date)

        return os.path.join('/tmp', file_name + '.csv')

    def output(self):
        """
        Luigi method: output target
        @return: luigi file ref
        """
        return CSVTarget(path=self.path, columns=self.get_output_columns())

    def on_success(self):

        log.info("You can import CSV file with:")
        log.info("COPY \"[RESOURCE ID]\" (\"{cols}\") FROM {path} DELIMITER ',' CSV ENCODING 'UTF8'".format(
           cols='","'.join(col[1] for col in self.columns if self._is_output_field(col[1])),
           path=self.path
        ))

        return super(DatasetToCSVTask, self).complete()

