#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import ckanapi
from ke2mongo.log import log
from ke2mongo import config


def ckan_delete(mongo_record):

        # Set up CKAN API connection
    ckan = ckanapi.RemoteCKAN(config.get('ckan', 'site_url'), apikey=config.get('ckan', 'api_key'))

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

    # Get the source primary key - this needs to be split on . as we have added the collection name
    ke_primary_key = primary_key_field[0].split('.')[1]

    # The name of the primary key field used in CKAN
    ckan_primary_key = primary_key_field[1]

    primary_key_value = mongo_record[ke_primary_key]

    # Load the resource, so we can find the resource ID
    resource = ckan.action.resource_show(id=task_cls.datastore['resource']['name'])

    # And delete the record from the datastore
    log.info('Deleting record from CKAN where %s=%s' % (ckan_primary_key, primary_key_value))
    ckan.action.datastore_delete(id=resource['id'], filters={ckan_primary_key: primary_key_value})