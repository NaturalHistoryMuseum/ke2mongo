#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2015-01-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Unpublish records marked non web publishable

python tasks/unpublish.py --local-scheduler --date 20150122

"""

import luigi
import ckanapi
from ke2mongo import config
from ke2mongo.log import log
from ke2mongo.lib.timeit import timeit
from ke2mongo.lib.ckan import ckan_delete
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.targets.mongo import MongoTarget

class UnpublishTask(luigi.Task):
    """
    If a KE EMu record has been marked non web publishable, it needs to be deleted from CKAN

    """

    date = luigi.IntParameter()
    database = config.get('mongo', 'database')
    keemu_schema_file = config.get('keemu', 'schema')

    # Set up CKAN API connection
    ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

    def requires(self):
        # Mongo catalogue task for date must have
        yield MongoCatalogueTask(self.date),


    @timeit
    def run(self):

        collection = self.output().get_collection('ecatalogue')

        cursor = collection.find({'exportFileDate': self.date, 'AdmPublishWebNoPasswordFlag': 'N'})

        for record in cursor:
            ckan_delete(record)

        # And mark the object as complete
        self.output().touch()

    def output(self):
        return MongoTarget(database=self.database, update_id=self.task_id)

if __name__ == "__main__":
    luigi.run(main_task_cls=UnpublishTask)
