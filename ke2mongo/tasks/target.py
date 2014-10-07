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

        # print args
        file_name = kwargs.pop('file_name')
        date = kwargs.pop('date', None)
        self.columns = kwargs.pop('columns')

        if date:
            file_name += '-' + str(date)

        kwargs['path'] = os.path.join('/tmp', file_name + '.csv')
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

    def __init__(self, package, datastore, columns, geospatial_fields=None, primary_key=None):

        self.package = package
        self.datastore = datastore
        self.columns = columns
        self.geospatial_fields = geospatial_fields
        self.primary_key = primary_key

        # Get resource id - and create datastore if it doesn't exist
        # Set up connection to CKAN
        self.ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

    def get_or_create_datastore(self):

        try:
            # If the package exists, retrieve the resource
            ckan_package = self.ckan.action.package_show(id=self.package['name'])
            resource_id = ckan_package['resources'][0]['id']

        except ckanapi.NotFound:

            log.info("Package %s not found - creating", self.package['name'])

            # Create the package
            self.datastore['resource']['package_id'] = self.ckan.action.package_create(**self.package)['id']
            # Add the field indexes
            # Add which fields should be indexed
            # TODO: Bugger. I need indexable
            # indexes = [col[1] for col in self.columns if col[3] and col[2].startswith('string')]
            fields = [{'id': col, 'type': self.numpy_to_ckan_type(np_type)} for col, np_type in self.columns.iteritems()]

            # self.datastore['indexes'] = indexes
            self.datastore['fields'] = fields

            if self.primary_key:
                self.datastore['primary_key'] = [self.primary_key]

            # API call to create the datastore
            resource_id = self.ckan.action.datastore_create(**self.datastore)['resource_id']

            # # If this has geospatial fields, create geom columns
            # if geospatial_fields:
            #     log.info("Creating geometry columns for %s", self.resource_id)
            #     geospatial_fields['resource_id'] = self.resource_id
            #     self.ckan.action.create_geom_columns(**geospatial_fields)

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
                ckan_type = 'text'
        except TypeError:
            # Strings are not objects, so we'll get a TypeError
            ckan_type = 'text'

        return ckan_type

    def exists(self):
        return False

    def write(self, df):

        self.resource_id = self.get_or_create_datastore()

        print "Saving records to CKAN"

        # Loop through all the dataframe columns, removing internal ones (starting with _)
        for col in df:
            if col.startswith('_'):
                df.drop(col, axis=1, inplace=True)

        print df['catalogue_number']

        df = df.where(pd.notnull(df), None)
        records = df.to_dict(outtype='records')

        datastore_params = {
            'resource_id': self.resource_id,
            'records': records
        }

        if self.primary_key:
            datastore_params['primary_key'] = self.primary_key

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

        # print datastore_params['records'][0]

        self.ckan.action.datastore_upsert(**datastore_params)

        # if self.datastore_exists():
        #     self.ckan.action.datastore_upsert(**datastore_params)
        # else:
        #     self.ckan.action.datastore_create(**datastore_params)

    def datastore_exists(self):
        sql = 'SELECT COUNT(*) as count FROM "{0}"'.format(self.resource_id)
        result = self.ckan.action.datastore_search_sql(sql=sql)
        return int(result['records'][0]['count']) > 0