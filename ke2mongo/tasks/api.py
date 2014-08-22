#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python task.py APITask  --local-scheduler

"""

import luigi
import ckanapi
import uuid


class APITask(luigi.Task):

    registry = ckanapi.RemoteCKAN('http://127.0.0.1:5000', apikey='953f968e-e68e-4f1f-a94a-d4d17fe5692f')

    package = {
            'name': u'api6',
            'notes': u'API',
            'title': "API",
            'author': 'Natural History Museum',
            'license_id': u'other-open',
            'resources': [],
            'dataset_type': 'Specimen',
            'owner_org': 'nhm'
        }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Test data',
            'description': 'Test data',
            'format': 'csv'
        },
    }


    def run(self):

        try:
            package = self.registry.action.package_show(id=self.package['name'])

            resource_id = package['resources'][0]['id']

        except ckanapi.NotFound:
            # Create the package
            package = self.registry.action.package_create(**self.package)
            # And create the datastore
            self.datastore['resource']['package_id'] = package['id']

            # API call to create the datastore
            datastore = self.registry.action.datastore_create(**self.datastore)

            resource_id = datastore['resource_id']

        print "CREATING DATA"

        records = []
        # Tested 100000 - Fails
        for i in range(2):
            record = {
                'id': i,
                'fruit': 'banana'
            }

            # for i in range (79):
            #     field = 'f%s' % i
            #     record[field] = str(uuid.uuid4())

            records.append(record)

        datastore_params = {
            'records': records,
            'resource_id': resource_id,
            'primary_key': 'id'
        }

        print "SAVING"

        self.registry.action.datastore_upsert(**datastore_params)

        print "RUN"


if __name__ == "__main__":
    luigi.run()