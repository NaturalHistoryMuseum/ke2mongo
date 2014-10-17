#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from ke2mongo.lib.timeit import timeit
from keparser import KEParser
from ke2mongo.log import log
from ke2mongo.tasks.mongo import MongoTask

class DeleteTask(MongoTask):
    """
    Import KE Taxonomy Export file into MongoDB
    """
    module = 'eaudit'
    file_extension = 'deleted-export'

    @timeit
    def run(self):

        # Build a dict of all modules and collections
        # We then retrieve the appropriate collection from the records module name (AudTable)
        collections = {}
        for cls in MongoTask.__subclasses__():
            collections[cls.module] = cls(None).get_collection()

        ke_data = KEParser(self.input().open('r'), schema_file=self.keemu_schema_file, input_file_path=self.input().path)

        # To avoid circular imports, import the tasks we need to check here
        # Dataset tasks are dependent on the DeleteTask
        from ke2mongo.tasks.indexlot import IndexLotDatasetTask
        from ke2mongo.tasks.artefact import ArtefactDatasetTask
        from ke2mongo.tasks.specimen import SpecimenDatasetToCKANTask

        for record in self.iterate_data(ke_data):

            module = record.get('AudTable')
            irn = record.get('AudKey')

            try:
                collection = collections[module]
            except KeyError:
                log.debug('Skipping eaudit record for %s' % module)
                # We do not have a collection for this module - skip to next record
                continue

            # Load record from MongoDB
            log.info('Load MongoDB: %s' % irn)

            mongo_record = collection.find_one({'_id': int(irn)})

            if mongo_record:

                # By default, use SpecimenDatasetToCKANTask
                task_cls = SpecimenDatasetToCKANTask

                # Override default if is Index Lot or Artefact
                for t in [IndexLotDatasetTask, ArtefactDatasetTask]:
                    if t.record_type == mongo_record['ColRecordType']:
                        task_cls = t
                        break

                print task_cls

        #
        #
        #
        #
        #
        #
        #         log.debug('Deleting record %s(%s)' % (module, irn))
        #         collection.remove({'_id': irn})
        #
        #     # TODO Delete
        #
        #
        #
        # self.mark_complete()
