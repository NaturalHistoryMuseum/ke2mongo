#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py CatalogueMongoTask --local-scheduler --date 2014-01-23

"""

from ke2mongo.tasks.mongo import MongoTask, InvalidRecordException
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES
from ke2mongo.log import log
from pymongo.errors import BulkWriteError

class MongoCatalogueTask(MongoTask):

    module = 'ecatalogue'

    # List of types to exclude
    excluded_types = [
        'Acquisition',
        'Bound Volume',
        'Bound Volume Page',
        'Collection Level Description',
        'DNA Card',  # 1 record, but keep an eye on this
        'Field Notebook',
        'Field Notebook (Double Page)',
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

    def process_record(self, data):

        # Only import if it's one of the record types we want
        record_type = data.get('ColRecordType', 'Missing')

        if record_type in self.excluded_types:
            log.debug('Skipping record %s: No model class for %s', data['irn'], record_type)
            raise InvalidRecordException
        else:
            return super(MongoCatalogueTask, self).process_record(data)

    def on_success(self):
        """
        On completion, add indexes
        @return: None
        """

        self.collection = self.get_collection()

        self.collection.ensure_index('ColRecordType')

        log.info("Updating child references")

        # Move to specimen_dataset
        self.add_child_refs()

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
        self.collection.update({'ColRecordType': {"$in": PARENT_TYPES}}, {"$unset": {"PartRef": None}}, multi=True)

        # # Add an index for child ref
        self.collection.ensure_index('ChildRef')

        result = self.collection.aggregate([
            {"$match": {"ColRecordType": {"$in": PARENT_TYPES + PART_TYPES}}},
            {"$group": {"_id": {"$ifNull": ["$RegRegistrationParentRef", "$_id" ]}, "ids": {"$addToSet": "$_id"}}},
            {"$match": {"ids.1": {"$exists": True}}}  # We only want records with more than one in the group
        ])

        bulk = self.collection.initialize_unordered_bulk_op()

        # Flag denoting if bulk has records and should be executed
        bulk_has_records = False

        for record in result['result']:
            try:
                record['ids'].remove(record['_id'])
            except ValueError:
                # Parent record does not exist
                # KE EMU obviously doesn't enforce referential integrity
                # We do not want to do anything with these records
                continue
            else:
                # Add updating record to the bulk process
                bulk.find({'_id': record['_id']}).update({'$set': {'PartRef': record['ids'], 'PartRefStr': '; '.join(map(str, record['ids']))}})
                bulk_has_records = True

        if bulk_has_records:
            result = bulk.execute()
            log.info('Added PartRef to %s parent records', result['nModified'])