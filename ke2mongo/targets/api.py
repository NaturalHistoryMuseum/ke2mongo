#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import pandas as pd
import numpy as np
import luigi
import ckanapi
import json
from ke2mongo.log import log
from ke2mongo import config

class APITarget(luigi.Target):

    def __init__(self, remote_ckan, resource_id, columns):

        self.remote_ckan = remote_ckan
        self.resource_id = resource_id
        self.columns = columns

    def exists(self):
        # Always run
        return False

    def write(self, df):

        log.info("Saving records to CKAN resource %s", self.resource_id)

        # Convert all empty/null values to None - so will be NULL values in postgres
        # Ensure any float fields with value 0.0 are actually None
        for col, np_type in self.columns.iteritems():

            if np_type.startswith('float'):
                df[col][df[col] == 0.0] = None
            else:
                # BUGFIX: Multimedia fields are being populated with empty string rather than NULL
                df[col][df[col].astype(str) == ''] = None

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
            'force': True
            # 'primary_key': '_id'
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
                    log.critical('Error encoding record: %s', ' '.join(['%s=%s' % (field, value) for field, value in record.iteritems() if value]))
                else:
                    validated_records.append(record)

            datastore_params['records'] = validated_records

        self.remote_ckan.action.datastore_upsert(**datastore_params)