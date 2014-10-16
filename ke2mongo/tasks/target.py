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


class CKANTarget(luigi.Target):

    def __init__(self, package, datastore, columns, geospatial_fields=None):

        self.package = package
        self.datastore = datastore
        self.columns = columns
        self.geospatial_fields = geospatial_fields

        # Get resource id - and create datastore if it doesn't exist
        # Set up connection to CKAN
        self.ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

    def get_or_create_datastore(self):

        resource_id = None

        try:
            # If the package exists, retrieve the resource
            ckan_package = self.ckan.action.package_show(id=self.package['name'])

            # Does a resource of the same name already exist for this dataset?
            # If it does, assign to resource_id
            for resource in ckan_package['resources']:
                if resource['name'] == self.datastore['resource']['name']:
                    resource_id = resource['id']
                    break

        except ckanapi.NotFound:
            log.info("Package %s not found - creating", self.package['name'])
            # Create the package
            ckan_package = self.ckan.action.package_create(**self.package)

        # If we don't have the resource ID, create
        if not resource_id:

            log.info("Resource %s not found - creating", self.datastore['resource']['name'])

            self.datastore['fields'] = [{'id': col, 'type': self.numpy_to_ckan_type(np_type)} for col, np_type in self.columns.iteritems()]
            self.datastore['resource']['package_id'] = ckan_package['id']

            # API call to create the datastore
            resource_id = self.ckan.action.datastore_create(**self.datastore)['resource_id']

            # If this has geospatial fields, create geom columns
            if self.geospatial_fields:
                log.info("Creating geometry columns for %s", resource_id)
                self.geospatial_fields['resource_id'] = resource_id
                self.ckan.action.create_geom_columns(**self.geospatial_fields)

            log.info("Created datastore resource %s", resource_id)

        return resource_id

    @staticmethod
    def numpy_to_ckan_type(pandas_type):
        """
        For a pandas field type, return s the corresponding ckan data type, to be used when creating datastore
        init32 => integer
        @param pandas_type: pandas data type
        @return: ckan data type
        """
        type_num, type_arg, numpy_type = get_monary_numpy_type(pandas_type)

        try:
            if issubclass(numpy_type, np.signedinteger):
                ckan_type = 'integer'
            elif issubclass(numpy_type, np.floating):
                ckan_type = 'float'
            elif numpy_type is bool:
                ckan_type = 'bool'
            else:
                # TODO: Add field type: citext
                ckan_type = 'text'
        except TypeError:
            # Strings are not objects, so we'll get a TypeError
            ckan_type = 'text'

        return ckan_type

    def exists(self):
        # Always run
        return False

    def write(self, df):

        self.resource_id = self.get_or_create_datastore()
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
            'primary_key': self.datastore['primary_key']
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