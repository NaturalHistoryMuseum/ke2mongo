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

class CSVTask(luigi.Task):
    """
    Class for exporting exporting KE Mongo data to CSV
    """
    # Name of the database
    database = luigi.Parameter()
    # Name of the collection
    collection_name = luigi.Parameter()
    # List of columns
    columns = luigi.Parameter(significant=False)
    # Query dictionary
    query = luigi.Parameter(significant=False)
    # Name of file to write CSV
    outfile = luigi.IntParameter(significant=False)
    # Callback for processing files
    process_callback = luigi.IntParameter(significant=False)

    @timeit
    def run(self):

        ke_cols, df_cols, inheritable, types = zip(*self.columns)

        # Number of records to retrieve (~200 breaks)
        block_size = 100
        # CSV Chunksize needs to be one more than block_size, so if we do get a UnicodeDecodeError, no rows will have been re-written
        csv_chunksize = block_size + 1

        mongo = MongoClient()
        db = mongo[self.database]

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

                db[self.collection_name].aggregate(query, allowDiskUse=True)

                query = {}

            elif not isinstance(query, dict):
                raise TypeError('Query needs to be either an aggregation list or query dict')

            with Monary() as m:

                catalogue_blocks = m.block_query(self.database, collection_name, query, ke_cols, types, block_size=block_size)

                for catalogue_block in catalogue_blocks:

                    # Loop through and ensure all output values are string, and empty values are ''
                    # If this isn't an output field, we will ignore it as empty values will not matter
                    catalogue_block = [arr.astype(np.str).filled('') if self.output_field(df_cols[i]) else arr for i, arr in enumerate(catalogue_block)]

                    # Create a pandas data frame with block of records
                    df = pd.DataFrame(np.matrix(catalogue_block).transpose(), columns=df_cols)

                    # Loop through all the columns and ensure hidden integer fields are cast as int32
                    # For example, taxonomy_irn is used to join with taxonomy df
                    for i, df_col in enumerate(df_cols):
                        if not self.output_field(df_col) and types[i] == 'int32':
                            df[df_col] = df[df_col].astype('int32')

                    self.process_callback(m, df)

                    row_count, col_count = df.shape

                    # Create list of columns to output
                    output_columns = self.output_columns()
                    # Write CSV file
                    try:
                        df.to_csv(self.output().path, chunksize=csv_chunksize, mode='a', columns=output_columns.keys(), index=False, header=False, encoding='utf-8')
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
                                df_row.to_csv(self.output().path, mode='a', columns=output_columns.keys(), index=False, header=False, encoding='utf-8')
                            except UnicodeDecodeError:
                                # On failure, log an error
                                log.critical('UTF8 Encoding error for record irn=%s', df_row.iloc[-1]['_id'])

                    count += row_count

                    log.info("\t %s records", count)

    def output_field(self, field):
        """
        Fields starting with _ are hidden and shouldn't be included in output (excluding _id)
        @param field:
        @return: bool
        """
        return field == '_id' or not field.startswith('_')

    def output_columns(self):
        # Dictionary of columns to output in the format field_name : type
        return OrderedDict((col[1], col[3]) for i, col in enumerate(self.columns) if self.output_field(col[1]))

    def output(self):
        return luigi.LocalTarget(self.outfile)