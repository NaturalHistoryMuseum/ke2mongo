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
from ke2mongo.tasks.api import APITask
from ke2mongo.targets.mongo import MongoTarget

class UnpublishTask(APITask):
    """
    If a KE EMu record has been marked non web publishable, it needs to be deleted from CKAN
    NB: This does not remove embargoed records which have already been published.
    You cannot embargo a record after it's release.
    """
    database = config.get('mongo', 'database')
    keemu_schema_file = config.get('keemu', 'schema')
    def requires(self):
        # Mongo catalogue task for date must have run
        yield MongoCatalogueTask(self.date)
    @timeit
    def run(self):
        # Do not run if this is a full export date - all non-publishable records will
        # Already have been removed
        if int(self.full_export_date) == int(self.date):
            log.info("No records to unpublish for full exports")
            self.mark_complete()
            return
        collection = self.output().get_collection('ecatalogue')
        q = dict(
            AdmPublishWebNoPasswordFlag='N',
            exportFileDate=self.date
        )
        cursor = collection.find(q)
        log.info('%s records to unpublish', cursor.count())

        for record in cursor:
            try:
                ckan_delete(self.remote_ckan, record)
            except ckanapi.NotFound:
                print record
                log.errpr('Record not found')

        # And mark the object as complete
        self.mark_complete()

    def mark_complete(self):
        self.output().touch()

    def output(self):
        return MongoTarget(database=self.database, update_id=self.task_id)

if __name__ == "__main__":
    luigi.run(main_task_cls=UnpublishTask)
