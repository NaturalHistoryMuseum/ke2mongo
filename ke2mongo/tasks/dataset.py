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
from ke2mongo.tasks.csv import CSVTask
import luigi
import abc
from monary.monary import get_monary_numpy_type
import numpy as np
from ke2mongo.log import log
import psycopg2
from ke2mongo.lib.timeit import timeit
from collections import OrderedDict

# TODO: This just copies data via postgres copy function - it's quick but need to do periodic updates etc., via API

class DatasetTask(luigi.Task):
    """
    Class for importing KE data into CKAN dataset
    """

    database = config.get('mongo', 'database')

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

    @abc.abstractproperty
    def collection_name(self):
        """
        Mongo collection name
        @return: str
        """
        return None

    @property
    def outfile(self):
        return os.path.join('/tmp', '%s.csv' % self.__class__.__name__.replace('DatasetTask', '').lower())

    def get_columns(self):
        """
        # Allow overriding columns
        @return: columns
        """
        return self.columns

    def requires(self):
        # Create a CSV export of this field data to be used in postgres copy command
        return CSVTask(database=self.database, collection_name=self.collection_name, query=self.query, columns=self.get_columns(), outfile=self.outfile)

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
            fields = [{'id': name, 'type': self.numpy_to_ckan_type(type)} for name, type in self.requires().csv_columns().items() if name not in ['_id']]

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

    @staticmethod
    def numpy_to_ckan_type(pandas_type):
        """
        For a pandas field type, return s the corresponding ckan data type, to be used when creating datastore
        init32 => integer
        @param pandas_type: pandas data type
        @return: ckan data type
        """
        type_num, type_arg, numpy_type = get_monary_numpy_type(pandas_type)

        # TODO: BOOL

        try:
            if issubclass(numpy_type, np.signedinteger):
                ckan_type = 'integer'
            elif issubclass(numpy_type, np.floating):
                ckan_type = 'float'
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

        datastore_config = dict(config.items('datastore'))

        conn = psycopg2.connect(**datastore_config)
        resource_id = self.get_resource_id()

        log.info("Copying CSV export to resource %s", resource_id)

        conn.cursor().execute("COPY \"{table}\"(\"{cols}\") FROM '{file}' DELIMITER ',' CSV".format(
            table=resource_id,
            cols='","'.join(self.requires().csv_columns().keys()),
            file=self.input().path
            )
        )

        conn.commit()








