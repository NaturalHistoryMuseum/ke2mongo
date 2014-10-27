#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
import datetime
from ke2mongo.lib.mongo import mongo_client_db, mongo_get_marker_collection_name

class MongoTarget(luigi.Target):

    marker_collection_name = mongo_get_marker_collection_name()

    def __init__(self, database, update_id):

        self.update_id = update_id
        # Set up a connection to the database
        self.db = mongo_client_db(database)
        # Use the postgres table name for the collection
        self.marker_collection = self.get_collection(self.marker_collection_name)

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