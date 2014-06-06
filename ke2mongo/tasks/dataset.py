#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import urllib2
import urllib
import json
from ke2mongo import config
import luigi
import abc
from monary.monary import get_monary_numpy_type
import numpy as np
from ke2mongo.log import log
import psycopg2
from ke2mongo.lib.timeit import timeit
from collections import OrderedDict
from ke2mongo.tasks import ARTEFACT_TYPE
from ke2mongo.tasks.csv import CSVTask

class DatasetTask(luigi.postgres.CopyToTable):
    """
    Class for importing KE data into CKAN dataset
    """

    # The data to process
    date = luigi.IntParameter()

    # Copy to table params
    host = config.get('datastore', 'host')
    database = config.get('datastore', 'database')
    user = config.get('datastore', 'user')
    password = config.get('datastore', 'password')

    # Default impl
    full_text_blacklist = []

    @abc.abstractproperty
    def name(self):
        """
        Name for this dataset
        @return: str
        """
        return None

    @abc.abstractproperty
    def description(self):
        """
        Description for this dataset
        @return: str
        """
        return None

    @abc.abstractproperty
    def format(self):
        """
        Format eg: DWC / CSV
        @return: str
        """
        return None

    @abc.abstractproperty
    def package(self):
        """
        Params for creating the package
        @return: str
        """
        return None

    @abc.abstractproperty
    def csv_class(self):
        """
        Class to use for generating CSV
        @return: str
        """
        return None

    @property
    def table(self):
        """
        Use table called _tmp_[resource_id]
        This will then replace the main resource table
        @return:
        """
        return '_tmp_%s' % self.resource_id

    @property
    def columns(self):
        """
        List of columns to use, based on the ones used to produce the CSV
        @return:
        """
        return self.csv.csv_output_columns().keys()

    def __init__(self, *args, **kwargs):
        """
        Override init to retrieve the resource id
        @param args:
        @param kwargs:
        @return:
        """

        super(DatasetTask, self).__init__(*args, **kwargs)
        self.resource_id = self.get_resource_id()


    def requires(self):
        # Create a CSV export of this field data to be used in postgres copy command
        self.csv = self.csv_class(date=self.date)
        return self.csv

    def api_call(self, action, data_dict):
        """
        API Call
        @param action: API action
        @param data: dict
        @return:
        """
        url = '{site_url}/api/3/action/{action}'.format(
            site_url=config.get('ckan', 'site_url'),
            action=action
        )

        # Use the json module to dump a dictionary to a string for posting.
        data_string = urllib.quote(json.dumps(data_dict))

        # New urllib request
        request = urllib2.Request(url)

        request.add_header('Authorization', config.get('ckan', 'api_key'))

        # Make the HTTP request.
        response = urllib2.urlopen(request, data_string)
        # Ensure we have correct response code 200
        assert response.code == 200

        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.read())

        # Check the contents of the response.
        assert response_dict['success'] is True
        result = response_dict['result']

        return result

    def get_resource_id(self):
        """
        Get the resource id for the dataset
        If the resource doesn't already exist, this function will create it
        @return: resource_id
        """
        try:
            package = self.api_call('package_show', {'id': self.package['name']})
        except urllib2.HTTPError:
            # Dataset does not exist, so create it now
            package = self.api_call('package_create', self.package)

        # Does a resource of the same name already exist for this dataset?
        # If it does, assign to resource_id
        resource_id = None
        for resource in package['resources']:
            if resource['name'] == self.name:
                resource_id = resource['id']
                break

        #  If the resource doesn't already exist, create it
        if not resource_id:

            # Dictionary of fields and field type
            fields = self.get_table_fields()

            # Parameters to create the datastore
            datastore_params = {
                'records': [],
                'resource': {
                    'name': self.name,
                    'description': self.description,
                    'package_id': package['id'],
                    'format': self.format
                },
                'fields': fields
            }

            # API call to create the datastore
            datastore = self.api_call('datastore_create', datastore_params)
            resource_id = datastore['resource_id']

        return resource_id

    def get_table_fields(self):
        """
        Get a list of all fields, in the format {'type': '[field type]', 'id': '[field name]'}
        @return: list
        """
        return [{'id': name, 'type': self.numpy_to_ckan_type(type)} for name, type in self.requires().csv_output_columns().items() if name not in ['_id']]


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

    @timeit
    def run(self):
        """
        Mongo has been written to CSV file - so upload to datastore
        @return:
        """

        # Get text fields
        fields = self.get_table_fields()

        full_text_fields = '","'.join([f['id'] for f in fields if f['type'] == 'text' and f['id'] not in self.full_text_blacklist])

        connection = self.output().connect()

        # Drop and recreate table
        self.create_table(connection)

        # And then copy the data to the new table
        cursor = connection.cursor()
        self.init_copy(connection)
        self.copy(cursor, self.input().path)

        log.info("Updating full text index for %s", self.resource_id)

        # Add _full_text index to the table
        cursor.execute(u'UPDATE "{table}" set _full_text = to_tsvector(ARRAY_TO_STRING(ARRAY["{full_text_fields}"], \' \'))'.format(table=self.table, full_text_fields=full_text_fields))

        self.table_replace_resource(connection)
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()

    def create_table(self, connection):
        """
        Override CopyToTable.create_table
        We already have the resource table in the datastore
        So we want to clone this table structure to use for the tmp import table
        @param connection:
        @return:
        """

        log.info("Creating temp table %s", self.table)

        cursor = connection.cursor()
        # Drop the table if it exists
        cursor.execute('DROP TABLE IF EXISTS "{table}"'.format(table=self.table))

        # And then recreate
        cursor.execute('CREATE TABLE "{table}" AS TABLE "{resource_id}" WITH NO DATA'.format(table=self.table, resource_id=self.resource_id))

    def table_replace_resource(self, connection):
        """
        Replace the existing resource table with the new one
        @param connection:
        @return:
        """
        log.info("Replacing resource table %s", self.resource_id)

        cursor = connection.cursor()

        # TODO: Copy table owner

        # Drop the resource table
        cursor.execute('DROP TABLE IF EXISTS "{resource_id}"'.format(resource_id=self.resource_id))

        # And rename temporary
        cursor.execute('ALTER table "{table}" RENAME TO "{resource_id}"'.format(table=self.table, resource_id=self.resource_id))


    def copy(self, cursor, file):

        cursor.execute("COPY \"{table}\"(\"{cols}\") FROM '{file}' DELIMITER ',' CSV".format(
            table=self.table,
            cols='","'.join(self.columns),
            file=file
            )
        )