#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py CatalogueMongoTask --local-scheduler --date 2014-01-23

"""

from ke2mongo.tasks.mongo import MongoTask
from ke2mongo.log import log

class CatalogueMongoTask(MongoTask):

    module = 'ecatalogue'

    # List of types to exclude
    excluded_types = [
        'Acquisition',
        'Bound Volume',
        'Bound Volume Page',
        'Collection Level Description',
        'DNA Card',  # 1 record, but keep an eye on this
        'Field Notebook',
        'Image',
        'Image (electronic)',
        'Image (non-digital)',
        'Image (digital)',
        'Incoming Loan',
        'L&A Catalogue',
        'Missing',
        'Object Entry',
        'object entry',  # FFS
        'Object entry',  # FFFS
        'PEG Specimen',
        'PEG Catalogue',
        'Preparation',
        'Rack File',
        'Tissue',  # Only 2 records. Watch.
        'Transient Lot'
    ]

    def process(self, data):

        # Only import if it's one of the record types we want
        record_type = data.get('ColRecordType', 'Missing')
        if record_type in self.excluded_types:
            log.debug('Skipping record %s: No model class for %s', data['irn'], record_type)
        else:
            super(CatalogueMongoTask, self).process(data)

    def on_success(self):
        """
        On completion, add indexes
        @return: None
        """
        self.collection.ensure_index('ColRecordType')