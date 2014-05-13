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
from ke2mongo import config
from ke2mongo.tasks.catalogue_mongo import CatalogueMongoTask
from collections import OrderedDict

class CSVTask(luigi.Task):
    """
    Class for exporting exporting KE Mongo data to CSV
    """
    # List of columns
    columns = luigi.Parameter()
    # Query dictionary
    query = luigi.Parameter()
    # Name of file to write CSV
    outfile = luigi.IntParameter()

    def run(self):

        t1 = time.time()

        database = config.get('mongo', 'database')
        collection_name = CatalogueMongoTask().collection_name()

        ke_cols, df_cols, types, output = zip(*self.columns)

        count = 0

        with Monary() as m:

            catalogue_blocks = m.block_query(database, collection_name, self.query, ke_cols, types)

            for catalogue_block in catalogue_blocks:

                # Loop through and ensure all output values are string, and empty values are ''
                # catalogue_block = [arr.astype(np.str).filled('') if output[i] else arr for i, arr in enumerate(catalogue_block)]

                # Create a pandas data frame with block of records
                df = pd.DataFrame(np.matrix(catalogue_block).transpose(), columns=df_cols)

                # Loop through all the columns and ensure not output integers are ints
                # For example, taxonomy_irn is used to join with taxonomy df
                for i, df_col in enumerate(df_cols):
                    if not output[i] and types[i] == 'int32':
                        df[df_col] = df[df_col].astype('int32')

                # TODO: Is this necessary - will postgres just do it for you? What if it fails?
                # df.convert_objects(convert_numeric=True)
                # df['DarDecimalLongitude'] = df['DarDecimalLongitude'].astype('float64')

                print df['DarDecimalLongitude']

                #  TODO: Floats are not coming out
                # df.convert_objects(convert_numeric=True)

                self.process(m, df)

                # Create list of columns to output
                output_columns = self.output_columns()

                # Write CSV file
                df.to_csv(self.output().path, chunksize=1000, mode='a', cols=output_columns.keys(), index=False, header=False)

                count += len(df)

                log.info("\t %s records", count)

        t2 = time.time()
        log.info('Time: %.2f secs', t2 - t1)

    def output_columns(self):
        # Dictionary of columns to output in the format field_name : type
        return OrderedDict((col[1], col[2]) for i, col in enumerate(self.columns) if col[3])

    def process(self, m, df):
        """
        Extensible function for processing dataframe
        @param m:
        @param df:
        @return: dataframe
        """
        return df

    def output(self):
        return luigi.LocalTarget(self.outfile)