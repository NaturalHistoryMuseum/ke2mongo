#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi
from ke2mongo.tasks.ke import KEFileTask
from ke2mongo.log import log
from keparser import KEParser
from keparser.parser import FLATTEN_ALL
from ke2mongo import config
from ke2mongo.lib.timeit import timeit
from pymongo import MongoClient
import abc

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
        self.marker_collection.insert({'update_id': self.update_id})


class InvalidRecordException(Exception):
    """
    Raise an exception for records we want to skip
    See MongoCatalogueTask.process()
    """
    pass


class MongoTask(luigi.Task):

    date = luigi.IntParameter()

    database = config.get('mongo', 'database')
    keemu_schema_file = config.get('keemu', 'schema')
    batch_size = 10
    collection = None

    @abc.abstractproperty
    def module(self):
        return None

    def requires(self):
        return KEFileTask(module=self.module, date=self.date)

    def collection_name(self):
        return self.module

    def get_collection(self):
        """
        Get a reference to the mongo collection object
        @return:
        """
        return self.output().get_collection(self.collection_name())

    @timeit
    def run(self):

        ke_data = KEParser(self.input().open('r'), schema_file=self.keemu_schema_file, input_file_path=self.input().path, flatten_mode=FLATTEN_ALL)

        self.collection = self.get_collection()

        # If we have any records in the collection, use bulk_update with mongo bulk upsert
        # Otherwise use batch insert (20% faster than using bulk insert())
        if self.collection.find_one():
            self.bulk_update(ke_data)
        else:
            self.batch_insert(ke_data)

        # Mark as complete
        self.output().touch()

    def bulk_update(self, ke_data):

        bulk = self.collection.initialize_unordered_bulk_op()

        for data in ke_data:
            try:
                data = self.process_record(data)
            except InvalidRecordException:
                continue
            else:
                bulk.find({'_id': data['_id']}).upsert().replace_one(data)

        bulk.execute()

    def batch_insert(self, ke_data):

        batch = []
        for data in ke_data:

            try:
                data = self.process_record(data)
            except InvalidRecordException:
                # If the data processing raises an invalid record exception, continue to next record
                continue
            else:

                if self.batch_size:
                    batch.append(data)

                    # If the batch length equals the batch size, commit and clear the batch
                    if len(batch) % self.batch_size == 0:
                        self.collection.insert(batch)
                        batch = []

                else:
                    self.collection.insert(data)

        # Add any records remaining in the batch
        if batch:
            self.collection.insert(self.batch)

    def process_record(self, data):

        # Use the IRN as _id & remove original
        data['_id'] = data['irn']
        # Keep the IRN but cast as string, so we can use it in $concat
        data['irn'] = str(data['irn'])
        return data

    def output(self):
        return MongoTarget(database='keemu', update_id=self.update_id())

    def update_id(self):
        """This update id will be a unique identifier for this insert on this collection."""
        return self.task_id