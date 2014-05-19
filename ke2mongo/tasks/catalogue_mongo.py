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

    # Parent record types
    # These will be excluded fr
    parent_types = [
        'Bird Group Parent',
        'Mammal Group Parent',
    ]

    part_types = [
        'Bird Group Part',
        'Egg',
        'Nest',
        'Mammal Group Part'
    ]

    def process(self, data):

        # Only import if it's one of the record types we want
        record_type = data.get('ColRecordType', 'Missing')
        if record_type in self.excluded_types:
            log.debug('Skipping record %s: No model class for %s', data['irn'], record_type)
        else:
            super(CatalogueMongoTask, self).process(data)

    def add_child_refs(self):
        """
        For parent / part records KE EMu has a reference to the parent on the Part - in field RegRegistrationParentRef
        However, for our Mongo aggregation pipeline, we need to have refs to the parts on the parent
        This function adds refs, in field PartRef (list)
        @return: none
        """

        # Set child ref to None for all parent type records
        # This ensures after updates records are kept up to date
        # We re-update all ChildRef fields below
        self.collection.update({'ColRecordType': {"$in": self.parent_types}}, {"$unset": {"PartRef": None}}, multi=True)

        # # Add an index for child ref
        self.collection.ensure_index('ChildRef')

        result = self.collection.aggregate([
            {"$match": {"ColRecordType": {"$in": self.parent_types + self.part_types}}},
            {"$group": {"_id": {"$ifNull": ["$RegRegistrationParentRef", "$_id" ]}, "ids": {"$addToSet": "$_id"}}},
            {"$match": {"ids.1": {"$exists": True}}}  # We only want records with more than one in the group
        ])

        bulk = self.collection.initialize_unordered_bulk_op()

        for record in result['result']:
            try:
                record['ids'].remove(record['_id'])
                # Add updating record to the bulk process
                bulk.find({'_id': record['_id']}).update({'$set': {'PartRef': record['ids']}})
            except ValueError:
                # Parent record does not exist
                # KE EMU obviously doesn't enforce referential integrity
                # We do not want to do anything with these records
                continue

        result = bulk.execute()

        log.info('Added PartRef to %s parent records', result['nModified'])

    def on_success(self):
        """
        On completion, add indexes
        @return: None
        """

        self.collection = self.get_collection()

        self.collection.ensure_index('ColRecordType')

        self.add_child_refs()



        # TODO: This isn't being marked as complete?

    # def collection_name(self):
    #     return 'ecatalogue_utf8'