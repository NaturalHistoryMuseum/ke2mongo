#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

 python tasks/mongo_catalogue.py --local-scheduler --date 20160303


"""

import luigi
from uuid import UUID
from ke2mongo.lib.cites import get_cites_species
from ke2mongo.tasks.mongo import MongoTask, InvalidRecordException
from ke2mongo.tasks import DATE_FORMAT
from ke2mongo.log import log

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

    cites_species = get_cites_species()

    def process_record(self, data):

        # Only import if it's one of the record types we want
        record_type = data.get('ColRecordType', 'Missing')

        if record_type in self.excluded_types:
            log.debug('Skipping record %s: Excluded type %s', data['irn'], record_type)
            raise InvalidRecordException

        # Make sure the UUID is valid

        guid = data.get('AdmGUIDPreferredValue', None)

        if guid:

            try:
                UUID(guid, version=4)
            except ValueError:
                # print 'Skipping: not a valid UUID'
                # Value error - not a valid hex code for a UUID.
                # continue
                print 'ERROR: ', guid
                raise InvalidRecordException

        # If we don't have collection department, skip it
        if not data.get('ColDepartment', None):
            raise InvalidRecordException

        date_inserted = data.get('AdmDateInserted', None)

        # Some records have an invalid AdmDateInserted=20-09-27
        # As we need this for the stats, we need to skip them - just checking against date length as it's much quicker
        if not date_inserted or len(DATE_FORMAT) != len(date_inserted):
            log.error('Skipping record %s: invalid AdmDateInserted %s', data['irn'], date_inserted)
            raise InvalidRecordException

        # For now, the mongo aggregator cannot handle int / bool in $concat
        # So properties that are used in dynamicProperties need to be cast as strings
        for i in ['DnaTotalVolume', 'FeaCultivated', 'MinMetRecoveryWeight', 'MinMetWeightAsRegistered']:
            if i in data:
                data[i] = str(data[i])

        # If record is a CITES species, mark cites = True
        scientific_name = data.get('DarScientificName', None)

        if scientific_name and scientific_name in self.cites_species:
            data['cites'] = True

        # Add embargoed date = 0 so we don't have to query against field exists (doesn't use the index)
        if not 'NhmSecEmbargoDate' in data:
            data['NhmSecEmbargoDate'] = 0
        return super(MongoCatalogueTask, self).process_record(data)

    def on_success(self):
        """
        On completion, add indexes
        @return: None
        """
        self.collection = self.get_collection()
        log.info("Adding ecatalogue indexes")
        self.collection.ensure_index('ColRecordType')
        # Only include active records - not Stubs etc.,
        self.collection.ensure_index('SecRecordStatus')
        # Add index on RegRegistrationParentRef - select records with the same parent
        self.collection.ensure_index('RegRegistrationParentRef')
        # Need to filter on web publishable
        self.collection.ensure_index('AdmPublishWebNoPasswordFlag')
        # Exclude records if they do not have a GUID
        self.collection.ensure_index('AdmGUIDPreferredValue')
        # Add embargo date index
        self.collection.ensure_index('NhmSecEmbargoDate')
        super(MongoCatalogueTask, self).on_success()

if __name__ == "__main__":
    luigi.run(main_task_cls=MongoCatalogueTask)