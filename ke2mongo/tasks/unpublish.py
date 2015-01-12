#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python tasks/unpublish.py --local-scheduler

"""

import luigi
from pymongo import MongoClient
from monary import Monary
from ke2mongo.log import log
from ke2mongo import config
from ke2mongo.lib.timeit import timeit
from ke2mongo.tasks.specimen import SpecimenDatasetAPITask
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask

class UnpublishTask(luigi.Task):

    host = config.get('mongo', 'host')
    database = config.get('mongo', 'database')
    collection = MongoCatalogueTask.module
    api_task = SpecimenDatasetAPITask()

    block_size = 100

    @property
    def query(self):

        # Use the base specimen query
        query = self.api_task.query
        query['AdmPublishWebPasswordFlag'] = 'N'

        return query

    @timeit
    def run(self):

        # Get the resource
        resource = self.api_task.ckan.action.resource_show(id=self.api_task.datastore['resource']['name'])

        total = MongoClient()[self.database][self.collection].find(self.query).count()

        log.info("%s to delete", total)

        count = 0

        primary_key_field = self.api_task.datastore['primary_key']

        with Monary(self.host) as m:

            record_blocks = m.block_query(self.database, MongoCatalogueTask.module, self.query, ['_id'], ['int32'], block_size=self.block_size)

            for record_block in record_blocks:
                for irn in record_block[0]:

                    key_value = self.api_task.primary_key_prefix + str(irn)

                    log.info(key_value)

                    self.api_task.ckan.action.datastore_delete(id=resource['id'], filters={primary_key_field: key_value})

                    return

                percentage = float(count)/float(total) * 100
                log.info("\tRecords\t\t{0}/{1} \t\test. {2:.1f}%".format(count, total, percentage))

if __name__ == "__main__":
    luigi.run(main_task_cls=UnpublishTask)







