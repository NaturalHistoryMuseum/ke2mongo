#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

"""

import os
import luigi
import numpy as np
import pandas as pd
import abc
import ckanapi
import itertools
from monary import Monary
from monary.monary import get_monary_numpy_type
from ke2mongo.log import log
from ke2mongo.lib.timeit import timeit
from ke2mongo import config
from collections import OrderedDict
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.delete import DeleteTask
from ke2mongo.lib.file import get_export_file_dates
from ke2mongo.tasks.target import CSVTarget, APITarget
from ke2mongo.tasks import  MULTIMEDIA_URL, MULTIMEDIA_FORMATS

class DatasetTask(luigi.Task):
    """
    Class for processing data mongo into a dataset
    If date set, this task requires all mongo files for that date to have been imported into Mongo DB
    """
    # Parameters
    # Date to process
    date = luigi.IntParameter(default=None)
    mongo_db = config.get('mongo', 'database')

    # MongoDB params
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
        Query object for selecting data from mongoDB
        @return: dict
        """

        query = {"ColRecordType": self.record_type}

        if self.date:
            query['exportFileDate'] = self.date

        return query

    # CKAN Dataset params
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

    def __init__(self, *args, **kwargs):

        # If a date parameter has been passed in, we'll just use that
        # Otherwise, loop through the files and get all dates
        super(DatasetTask, self).__init__(*args, **kwargs)

        export_file_dates = get_export_file_dates()
        # If we have more than one file export date, it could be problem if one of the mongo import files
        # So raise an exception, and ask the user to run manually
        if len(export_file_dates) > 1:
            raise IOError('There are multiple (%s) export files requiring processing. Please investigate and run bulk.py' % len(export_file_dates))

        # Create CKAN API instance
        self.ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

        # Get or create the resource object
        self.resource_id = self.get_or_create_resource()

    def requires(self):
        # Call all mongo tasks to import latest mongo data dumps
        # If a file is missing, the process will terminate with an Exception
        # These run in reverse order, so MongoCatalogueTask runs last

        # Only require mongo tasks if data parameter is passed in - allows us to rerun for testing
        if self.date:
            yield MongoCatalogueTask(self.date), DeleteTask(self.date), MongoTaxonomyTask(self.date),  MongoMultimediaTask(self.date), MongoMultimediaTask(self.date)

    def get_or_create_resource(self):
        """

        Either load a resource object
        Or if it doesn't exist, create the dataset package, and datastore

        @param package: params to create the package
        @param datastore: params to create the datastore
        @return: CKAN resource ID
        """

        resource_id = None

        try:
            # If the package exists, retrieve the resource
            ckan_package = self.ckan.action.package_show(id=self.package['name'])

            # Does a resource of the same name already exist for this dataset?
            # If it does, assign to resource_id
            for resource in ckan_package['resources']:
                if resource['name'] == self.datastore['resource']['name']:
                    self.validate_resource(resource)
                    resource_id = resource['id']

        except ckanapi.NotFound:
            log.info("Package %s not found - creating", self.package['name'])
            # Create the package
            ckan_package = self.ckan.action.package_create(**self.package)

        # If we don't have the resource ID, create
        if not resource_id:
            log.info("Resource %s not found - creating", self.datastore['resource']['name'])

            self.datastore['fields'] = [{'id': col, 'type': self.numpy_to_ckan_type(np_type)} for col, np_type in self.get_output_columns().iteritems()]
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

    def validate_resource(self, resource):
        # Validate the resource - see DatasetCSVTask
        # Raise Exception on failure
        pass  # default impl

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

    @staticmethod
    def ensure_multimedia(m, df, multimedia_field):

        # The multimedia field contains IRNS of all items - not just images
        # So we need to look up the IRNs against the multimedia record to get the mime type
        # And filter out non-image mimetypes we do not support

        # Convert associatedMedia field to a list
        df[multimedia_field] = df[multimedia_field].apply(lambda x: list(int(z.strip()) for z in x.split(';') if z.strip()))

        def get_valid_multimedia(multimedia_irns):
            """
            Get a data frame of taxonomy records
            @param m: Monary instance
            @param irns: taxonomy IRNs to retrieve
            @return:
            """
            q = {'_id': {'$in': multimedia_irns}, 'MulMimeFormat': {'$in': MULTIMEDIA_FORMATS}}
            ('_id', '_taxonomy_irn', 'int32'),
            query = m.query('keemu', 'emultimedia', q, ['_id'], ['int32'])
            return query[0].view()

        # Get a unique list of IRNS
        unique_multimedia_irns = list(set(itertools.chain(*[irn for irn in df[multimedia_field].values])))

        # Get a list of multimedia irns with valid mimetypes
        valid_multimedia = get_valid_multimedia(unique_multimedia_irns)

        # And finally update the associatedMedia field, so formatting with the IRN with MULTIMEDIA_URL, if the IRN is in valid_multimedia
        df[multimedia_field] = df[multimedia_field].apply(lambda irns: '; '.join(MULTIMEDIA_URL % irn for irn in irns if irn in valid_multimedia))

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


class DatasetAPITask(DatasetTask):
    """
    Write directly to CKAN API
    """
    def output(self):
        return APITarget(resource_id=self.resource_id, columns=self.get_output_columns())

# If this works, all tasks will be CKAN tasks.
# So move dataset creation to DatasetTask

class DatasetCSVTask(DatasetTask):
    """
    Output dataset to CSV
    """
    @property
    def path(self):
        """
        File name to output
        @return: str
        """
        file_name = self.__class__.__name__.replace('DatasetCSVTask', '').lower()

        if self.date:
            file_name += '-' + str(self.date)

        return os.path.join('/tmp', file_name + '.csv')

    def output(self):
        """
        Luigi method: output target
        @return: luigi file ref
        """
        return CSVTarget(path=self.path, columns=self.get_output_columns())

    def validate_resource(self, resource):
        """
        Validate the resource
        Make sure the fields we're creating in the CSV are the same as in the dataset
        @param resource: resource dict
        @return: None
        """

        # Load the datastore fields (limit = 0 so no rows returned)
        datastore = self.ckan.action.datastore_search(resource_id=resource['id'], limit=0)

        # Create a list of all (non-internal - _id) datastore fields we'd expect in the CSV
        datastore_fields = [field['id'] for field in datastore['fields'] if field['id'] != '_id']
        columns = [col for col in self.get_output_columns().keys()]
        assert datastore_fields == columns, 'Current datastore fields do not match CSV fields'

    def on_success(self):

        log.info("Import CSV file with:")
        log.info("COPY \"{resource_id}\" (\"{cols}\") FROM '{path}' DELIMITER ',' CSV ENCODING 'UTF8'".format(
            resource_id=self.resource_id,
            cols='","'.join(col[1] for col in self.columns if self._is_output_field(col[1])),
            path=self.path
        ))

        return super(DatasetCSVTask, self).complete()