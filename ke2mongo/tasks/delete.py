#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from luigi.task import getpaths
from keparser import KEParser
from ke2mongo import config
from ke2mongo.log import log
from ke2mongo.lib.timeit import timeit
from ke2mongo.lib.ckan import ckan_delete
from ke2mongo.tasks.api import APITask

# Need all mongo tasks, as we dynamically retrieve the collections
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_collection_event import MongoCollectionEventTask
from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.ke import KEFileTask


class DeleteAPITask(APITask, MongoDeleteTask):
    """
    Delete Task for deleting from mongo and
    """

    # Date is required for delete task
    date = luigi.IntParameter()

    # This delete task does delete records from CKAN so override force to True
    force = True

    def requires(self):

        # For delete to run, all other mongo tasks for same date must have already run
        # As on delete we also remove from MongoDB
        return [
            MongoCatalogueTask(date=self.date),
            MongoTaxonomyTask(self.date),
            # MongoMultimediaTask(self.date),
            MongoCollectionIndexTask(self.date),
            MongoCollectionEventTask(self.date),
            MongoSiteTask(self.date),
            # And then add the normal Delete requirement - processing the KE File
            super(DeleteAPITask, self).requires()
        ]

    def input(self):
        """
        Override task.input()
        Loop through each of the requirements - only the KEFileTask is the input
        """
        for i in self.requires():
            if isinstance(i, KEFileTask):
                return getpaths(i)

    def delete(self, collection, irn):

        # If this is an ecatalogue record, try and delete from CKAN
        if collection.name == 'ecatalogue':

            # Load the record from mongo
            mongo_record = collection.find_one({'_id': int(irn)})

            if mongo_record:
                ckan_delete(self.remote_ckan, mongo_record)
            else:
                log.info('Record %s does not exist. SKipping delete.' % irn)

        # And call the Mongo Delete task delete() method to remove the record from mongodb
        super(DeleteAPITask, self).delete(collection, irn)


if __name__ == "__main__":
    luigi.run(main_task_cls=DeleteAPITask)
