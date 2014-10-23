#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
import ckanapi
from ke2mongo import config
from ke2mongo.log import log
from ke2mongo.tasks.mongo_delete import MongoDeleteTask

class DeleteTask(MongoDeleteTask):
    """
    Extends MongoDeleteTask to also delete from CKAN
    """

    # Use the same task family as MongoDeleteTask
    # if MongoDeleteTask has run, we do not want this to run, and vice versa
    task_family = MongoDeleteTask.task_family

    # Set up CKAN API connection
    ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

    def delete(self, collection, irn):

        # If this is an ecatalogue record, try and delete from CKAN
        if collection.name == 'ecatalogue':
            self.ckan_delete(collection, irn)

        super(DeleteTask, self).delete(collection, irn)

    def ckan_delete(self, collection, irn):

        # Load record from MongoDB
        log.info('Load MongoDB record %s' % irn)

        # Load the record from mongo
        mongo_record = collection.find_one({'_id': int(irn)})

        if mongo_record:

            # To avoid circular imports, import the tasks we need to check here
            # Dataset tasks are dependent on the DeleteTask
            from ke2mongo.tasks.indexlot import IndexLotDatasetAPITask
            from ke2mongo.tasks.artefact import ArtefactDatasetAPITask
            from ke2mongo.tasks.specimen import SpecimenDatasetAPITask

            # By default, use SpecimenDatasetAPITask
            task_cls = SpecimenDatasetAPITask

            # Override default class if is Index Lot or Artefact
            for t in [IndexLotDatasetAPITask, ArtefactDatasetAPITask]:
                if t.record_type == mongo_record['ColRecordType']:
                    task_cls = t
                    break

            # Initiate the task class so we can access values and methods
            task = task_cls()
            primary_key_field = task.get_primary_key_field()
            primary_key = mongo_record[primary_key_field[0]]

            # If we have a primary key prefix, append it and ensure primary key is a string
            if task.primary_key_prefix:
                primary_key = task.primary_key_prefix + str(primary_key)

            # Load the resource, so we can find the resource ID
            resource = self.ckan.action.resource_show(id=task_cls.datastore['resource']['name'])

            # And delete the record from the datastore
            log.info('Deleting record from CKAN where %s=%s' % (primary_key_field[1], primary_key))
            self.ckan.action.datastore_delete(id=resource['id'], filters={primary_key_field[1]: primary_key})

if __name__ == "__main__":
    luigi.run(main_task_cls=DeleteTask)
