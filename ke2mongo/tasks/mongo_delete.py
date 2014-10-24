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

# Need all mongo tasks, as we dynamically retrieve the collections
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_site import MongoSiteTask

class MongoDeleteTask(MongoTask):
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

        for record in self.iterate_data(ke_data):

            module = record.get('AudTable')
            irn = record.get('AudKey')
            try:
                collection = collections[module]
            except KeyError:
                log.debug('Skipping eaudit record for %s' % module)
                # We do not have a collection for this module - skip to next record
                continue
            else:
                log.info('Deleting record %s(%s)' % (module, irn))
                self.delete(collection, irn)

        self.mark_complete()

    def delete(self, collection, irn):
        """
        Delete record from collection
        @param collection:
        @param irn:
        @return:
        """
        collection.remove({'_id': irn})


if __name__ == "__main__":
    luigi.run(main_task_cls=MongoDeleteTask)