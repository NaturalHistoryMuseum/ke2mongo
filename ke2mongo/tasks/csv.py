#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi
import time
import numpy as np
import pandas as pd
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
import abc
from ke2mongo.lib.file import get_export_file_dates

class CSVTask(luigi.Task):
    """
    Class for exporting exporting KE Mongo data to CSV
    This requires all mongo files have been imported into  Mongo DB
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
        Name for this dataset
        @return: str
        """
        return None

    def __init__(self, *args, **kwargs):

        # If a date parameter has been passed in, we'll just use that
        # Otherwise, loop through the files and get all dates
        super(CSVTask, self).__init__(*args, **kwargs)

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
            yield MongoCatalogueTask(self.date), MongoDeleteTask(self.date), MongoTaxonomyTask(self.date), MongoMultimediaTask(self.date)

    @timeit
    def run(self):

        # Number of records to retrieve (~200 breaks)
        block_size = 100
        # CSV Chunksize needs to be one more than block_size, so if we do get a UnicodeDecodeError, no rows will have been re-written
        csv_chunksize = block_size + 1

        mongo = MongoClient()
        db = mongo[self.mongo_db]

        # Ensure self.query is a tuple (luigi list params are converted to tuples)
        if not isinstance(self.query, list):
            self.query = [self.query]

        for query in self.query:

            count = 0

            # Default collection name. This can be over-ridden by the $out setting in an aggregator
            collection_name = self.collection_name

            # Is this query object a list?
            if isinstance(query, list):

                # This is an aggregator, so needs building and the query will return everything ({})

                # The last element needs to be the out collection
                out = query[len(query) - 1]

                # Set the collection name to the out collection.
                # If last key isn't $out, this will raise an exception
                collection_name = out['$out']

                # Run the aggregation query
                log.info("Building aggregated collection: %s", collection_name)

                result = db[self.collection_name].aggregate(query, allowDiskUse=True)

                # Ensure the aggregation process succeeded
                assert result['ok'] == 1.0

                query = {}

            elif not isinstance(query, dict):
                raise TypeError('Query needs to be either an aggregation list or query dict')

            with Monary() as m:

                log.info("Exporting mongo db to CSV")

                query_fields, df_cols, field_types = zip(*self.get_columns())

                catalogue_blocks = m.block_query(self.mongo_db, collection_name, query, query_fields, field_types, block_size=block_size)

                for catalogue_block in catalogue_blocks:

                    # Columns are indexed by key in the catalogue
                    catalogue_block = [arr.astype(np.str).filled('') if self.output_field(df_cols[i]) else arr for i, arr in enumerate(catalogue_block)]

                    # Create a pandas data frame with block of records
                    # Columns use the name from the output columns - but must be in the same order as query_fields
                    # Which is why we're using tuples for the columns
                    df = pd.DataFrame(np.matrix(catalogue_block).transpose(), columns=df_cols)

                    # Loop through all the columns and ensure hidden integer fields are cast as int32
                    # For example, taxonomy_irn is used to join with taxonomy df
                    for i, df_col in enumerate(df_cols):
                        if not self.output_field(df_col) and field_types[i] == 'int32':
                            df[df_col] = df[df_col].astype('int32')

                    df = self.process_dataframe(m, df)

                    row_count, col_count = df.shape

                    # Create list of columns to output
                    # As the df columns are indexed by column name, these don't have to align with the frame
                    csv_columns = self.csv_output_columns()

                    # print csv_columns

                    # Write CSV file
                    try:
                        df.to_csv(self.output().path, chunksize=csv_chunksize, mode='a', columns=csv_columns.keys(), index=False, header=False, encoding='utf-8')
                    except UnicodeDecodeError:

                        # Batch writing to CSV failed - rather than ditch the whole batch, loop through and write each individually, logging an error for failures
                        # Some of these failures are just corrupt records in KE EMu - for example record irn has
                        # For example: DarFieldNumber:1=ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢

                        # Loop through each row
                        for i in range(row_count):
                            # Get one row of the dataframe as new frame
                            df_row = df[i:i+1]
                            try:
                                # Try to write the row
                                df_row.to_csv(self.output().path, mode='a', columns=csv_columns.keys(), index=False, header=False, encoding='utf-8')
                            except UnicodeDecodeError:
                                # On failure, log an error with the _id of that row
                                log.critical('UTF8 Encoding error for record irn=%s', df_row.iloc[-1]['_id'])

                    count += row_count

                    log.info("\t %s records", count)

    def get_columns(self):
        """
        # Allow overriding columns
        @return: columns
        """
        return self.columns

    def process_dataframe(self, m, df):
        return df  # default impl

    @staticmethod
    def output_field(field):
        """
        Fields starting with _ are hidden and shouldn't be included in output (excluding _id)
        @param field:
        @return: bool
        """
        return field == '_id' or not field.startswith('_')

    def csv_output_columns(self):
        """
        Columns to output to CSV - overrideable
        @return: Dictionary field_name : type
        """
        return self._map_csv_columns(self.columns)

    def _map_csv_columns(self, columns):
        """
        Map CSV columns to a dictionary
        @param columns: list
        @return: dict
        """
        return OrderedDict((col[1], col[2]) for col in columns if self.output_field(col[1]))

    def output(self):
        """
        Luigi method: output target
        @return: luigi file ref
        """

        output_file = self.__class__.__name__.lower()
        if self.date:
            output_file += '_' + str(self.date)

        return luigi.LocalTarget("/tmp/%s.csv" % output_file)