#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

"""

import sys
import os
import luigi
import datetime
import abc
from luigi.parameter import ParameterException
from ke2mongo.tasks.ke import KEFileTask
from ke2mongo.log import log
from keparser import KEParser
from keparser.parser import FLATTEN_NONE, FLATTEN_SINGLE, FLATTEN_ALL
from ke2mongo import config
from ke2mongo.lib.timeit import timeit
from pymongo import MongoClient
from pymongo.errors import InvalidOperation
from ConfigParser import NoOptionError


class MongoTarget(luigi.Target):

    def __init__(self, database, update_id):

        self.update_id = update_id
        # Set up a connection to the database
        self.client = MongoClient()
        self.db = self.client[database]
        # Use the postgres table name for the collection
        self.marker_collection = self.get_collection(luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates'))

    def get_collection(self, collection):
        return self.db[collection]

    def exists(self):
        """
        Has this already been processed?
        """
        exists = self.marker_collection.find({'update_id': self.update_id}).count()
        return bool(exists)

    def touch(self):
        """
        Mark this update as complete.
        """
        self.marker_collection.insert({'update_id': self.update_id, 'inserted': datetime.datetime.now()})


class InvalidRecordException(Exception):
    """
    Raise an exception for records we want to skip
    See MongoCatalogueTask.process_record()
    """
    pass

class FlattenModeParameter(luigi.Parameter):
    """Parameter whose value is one of FLATTEN_NONE, FLATTEN_SINGLE, FLATTEN_ALL"""

    flatten_modes = [FLATTEN_NONE, FLATTEN_SINGLE, FLATTEN_ALL]

    def parse(self, s):

        s = int(s)

        if not s in self.flatten_modes:
            raise ParameterException('Flatten mode must be one of %s' % ' '.join([str(m) for m in self.flatten_modes]))

        return s


class MongoTask(luigi.Task):

    date = luigi.IntParameter()
    # Added parameter to allow skipping the processing of records - this is so MW can look at the raw data in mongo
    process = luigi.BooleanParameter(default=True)
    flatten_mode = FlattenModeParameter(default=FLATTEN_ALL)

    database = config.get('mongo', 'database')
    keemu_schema_file = config.get('keemu', 'schema')

    batch_size = 1000
    bulk_op_size = 100000
    collection = None
    file_extension = 'export'

    @abc.abstractproperty
    def module(self):
        return None

    @property
    def collection_name(self):
        return self.module  # By default, the collection name will be the same as the module

    def requires(self):
        return KEFileTask(module=self.module, date=self.date, file_extension=self.file_extension)

    def get_collection(self):
        """
        Get a reference to the mongo collection object
        @return:
        """
        return self.output().get_collection(self.collection_name)

    @timeit
    def run(self):

        ke_data = KEParser(self.input().open('r'), schema_file=self.keemu_schema_file, input_file_path=self.input().path, flatten_mode=self.flatten_mode)
        self.collection = self.get_collection()

        # If we have any records in the collection, use bulk_update with mongo bulk upsert
        # Otherwise use batch insert (20% faster than using bulk insert())
        if self.collection.find_one():
            self.bulk_update(ke_data)
        else:
            self.batch_insert(ke_data)

        self.mark_complete()

    def mark_complete(self):

        # Move the file to the archive directory (if specified)
        try:
            archive_dir = config.get('keemu', 'archive_dir')
            self.input().move(os.path.join(archive_dir, self.input().file_name))
        except NoOptionError:
            # Allow archive dir to be none
            pass

        # And mark the object as complete
        self.output().touch()

    def bulk_update(self, ke_data):

        bulk = self.collection.initialize_unordered_bulk_op()

        count = 0

        for record in self.iterate_data(ke_data):

            # Find and replace doc - inserting if it doesn't exist
            bulk.find({'_id': record['_id']}).upsert().replace_one(record)
            count += 1

            # Bulk ops can have out of memory errors (I'm getting for ~400,000+ bulk ops)
            # So execute the bulk op in stages, when bulk_op_size is reached
            if count % self.bulk_op_size == 0:
                log.info('Executing bulk op')
                bulk.execute()
                bulk = self.collection.initialize_unordered_bulk_op()

        try:
            bulk.execute()
        except InvalidOperation:
            # If we do not have any records to execute, ignore error
            # They have been executed in ln132
            pass

    def batch_insert(self, ke_data):

        batch = []

        for record in self.iterate_data(ke_data):

            if self.batch_size:
                batch.append(record)

                # If the batch length equals the batch size, commit and clear the batch
                if len(batch) % self.batch_size == 0:
                    log.info('Submitting batch')
                    self.collection.insert(batch)
                    batch = []

            else:
                self.collection.insert(record)

        # Add any records remaining in the batch
        if batch:
            self.collection.insert(batch)

    def iterate_data(self, ke_data):
        """
        Iterate through the data
        @return:
        """
        for record in ke_data:

            status = ke_data.get_status()

            if status:
                log.info(status)



            try:
                # Only process if flag is set (default=True)
                if self.process:
                    record = self.process_record(record)
            except InvalidRecordException:
                continue
            else:
                yield record

    def process_record(self, data):

        # Use the IRN as _id & remove original
        data['_id'] = data['irn']
        # Keep the IRN but cast as string, so we can use it in $concat
        data['irn'] = str(data['irn'])

        # Add the date of the export file
        data['exportFileDate'] = self.date

        return data

    def output(self):
        return MongoTarget(database=self.database, update_id=self.update_id())

    def update_id(self):
        """This update id will be a unique identifier for this insert on this collection."""
        return self.task_id

    def on_success(self):
        """
        On completion, add indexes
        @return: None
        """

        self.collection = self.get_collection()

        log.info("Adding exportFileDate index")

        self.collection.ensure_index('exportFileDate')
