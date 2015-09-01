#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import ckanapi
from ke2mongo.log import log

def ckan_delete(remote_ckan, mongo_record):

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

    # Get the primary key
    for col in task_cls.columns:
        if col[1] == task_cls.datastore['primary_key']:
            primary_key_field = col
            break

    # Get the source primary key - this needs to be split on . as we have added the collection name
    ke_primary_key = primary_key_field[0].split('.')[1]

    # The name of the primary key field used in CKAN
    ckan_primary_key = primary_key_field[1]

    primary_key_value = mongo_record[ke_primary_key]

    # Load the package, so we can find the resource ID
    try:
        ckan_package = remote_ckan.action.package_show(id=task_cls.package['name'])
        resource = ckan_package['resources'][0]
    except ckanapi.NotFound, e:
        print e
        log.error('CKAN Package %s not found', task_cls.package['name'])
        raise
    except ckanapi.CKANAPIError, e:
        print e
        print task_cls.package
        log.error('CKAN API ERROR', task_cls.package['name'])
        raise

    try:
        # And delete the record from the datastore
        log.info('Deleting record from CKAN where %s=%s' % (ckan_primary_key, primary_key_value))
        remote_ckan.action.datastore_delete(id=resource['id'], filters={ckan_primary_key: primary_key_value}, force=True)
    except ckanapi.CKANAPIError:
        # We don't care if the record isn't found
        log.error('Record not found')