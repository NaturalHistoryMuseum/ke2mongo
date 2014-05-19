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
from pymongo import MongoClient
import abc
import time


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


class MongoTask(luigi.Task):

    date = luigi.DateParameter(default=None)

    database = config.get('mongo', 'database')
    keemu_schema_file = config.get('keemu', 'schema')
    batch_size = 1000
    batch = []
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

    def run(self):

        t1 = time.time()

        ke_data = KEParser(self.input().open('r'), schema_file=self.keemu_schema_file, input_file_path=self.input().path, flatten_mode=FLATTEN_ALL)

        self.collection = self.get_collection()

        for data in ke_data:

            status = ke_data.get_status()

            if status:
                print(status)

            self.process(data)

        # Add any remaining records in the batch
        if self.batch:
            self.collection.insert(self.batch)

        # Mark as complete
        self.output().touch()

        t2 = time.time()
        log.info('Time: %.2f secs', t2 - t1)

    def process(self, data):

        # Use the IRN as _id & remove original
        data['_id'] = data['irn']
        # Keep the IRN but cast as string, so we can use it in $concat
        data['irn'] = str(data['irn'])

        if self.batch_size:
            self.batch.append(data)

            if len(self.batch) % self.batch_size == 0:
                self.collection.insert(self.batch)
                self.batch = []

        else:
            self.collection.insert(data)

    def output(self):
        return MongoTarget(database='keemu', update_id=self.update_id())

    def update_id(self):
        """This update id will be a unique identifier for this insert on this collection."""
        return self.task_id