#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
import pandas as pd
import numpy as np
import luigi
import ckanapi
import json
from ke2mongo.log import log
from ke2mongo import config
from monary.monary import get_monary_numpy_type

class CSVTarget(luigi.LocalTarget):
    """
    Output a dataset df to CSV
    """
    def __init__(self, *args, **kwargs):

        # Remove custom kwargs before passing to luigi.LocalTarget.__init__()
        self.columns = kwargs.pop('columns')
        super(CSVTarget, self).__init__(*args, **kwargs)

    def write(self, df):

        row_count, col_count = df.shape

         # CSV Chunksize needs to be one more than block_size, so if we do get a UnicodeDecodeError, no rows will have been re-written
        chunksize = row_count + 1

        try:
            df.to_csv(self.path, chunksize=chunksize, mode='a', columns=self.columns.keys(), index=False, header=False, encoding='utf-8')
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
                    df_row.to_csv(self.path, mode='a', columns=self.columns.keys(), index=False, header=False, encoding='utf-8')
                except UnicodeDecodeError:
                    # On failure, log an error with the _id of that row
                    log.critical('UTF8 Encoding error for record irn=%s', df_row.iloc[-1]['_id'])


class APITarget(luigi.Target):

    def __init__(self, resource_id, columns):

        self.resource_id = resource_id
        self.columns = columns

        # Set up connection to CKAN
        self.ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

    def exists(self):
        # Always run
        return False

    def write(self, df):

        log.info("Saving records to CKAN resource %s", self.resource_id)

        for col, np_type in self.columns.iteritems():
            if np_type.startswith('float'):
                # Ensure any float fields with value 0.0 are actually None
                df[col][df[col] == '0.0'] = np.NaN
            elif np_type.startswith('bool'):
                # And make sure '' in boolean fields are also None
                df[col][df[col] == ''] = np.NaN

        # Loop through all the dataframe columns, removing internal ones (fields starting with _)
        for col in df:
            if col.startswith('_'):
                df.drop(col, axis=1, inplace=True)

        # Convert all NaN to None
        df = df.where(pd.notnull(df), None)

        # Convert records to dictionary
        records = df.to_dict(outtype='records')

        datastore_params = {
            'resource_id': self.resource_id,
            'records': records,
            # 'primary_key': self.datastore['primary_key']
        }

        # Check that the data doesn't contain invalid chars
        try:
            json.dumps(datastore_params).encode('ascii')
        except UnicodeDecodeError:
            # At least one of the records contain invalid chars
            # Loop through, validating each of the records

            validated_records = []

            for i, record in enumerate(datastore_params['records']):
                try:
                    json.dumps(record).encode('ascii')
                except UnicodeDecodeError:
                    print 'Error encoding record'
                    print record
                else:
                    validated_records.append(record)

            datastore_params['records'] = validated_records

        self.ckan.action.datastore_upsert(**datastore_params)