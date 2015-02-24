#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from luigi.parameter import MissingParameterException
from ke2mongo.lib.timeit import timeit
from keparser import KEParser
from ke2mongo.log import log
from ke2mongo.tasks.mongo import MongoTask

# Import all the mongo tasks so they're findable in MongoTask.__subclasses__()
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_collection_event import MongoCollectionEventTask
from ke2mongo.tasks.mongo_site import MongoSiteTask

class MongoDeleteTask(MongoTask):
    """
    Delete records from Mongo DB
    This does not delete the corresponding records from CKAN dataset
    This should only be used prior to rebuilding the entire dataset

    To ensure it's not called in error, it must be called with flag force

    python tasks/mongo_delete.py --date 20140821 --local-scheduler --force

    """

    module = 'eaudit'
    file_extension = 'deleted-export'

    force = luigi.BooleanParameter(default=False, significant=False)

    @timeit
    def run(self):

        # Running this task doesn't delete anything from CKAN itself - so require --force flag to be sent to run it
        if not self.force:
            raise Exception('Warning: this class does not delete CKAN records. Use --force to run it.')

        # Build a dict of all modules and collections
        # We then retrieve the appropriate collection from the records module name (AudTable)
        # Exclude the MongoDeleteTask though
        collections = {cls.module: cls(None).get_collection() for cls in MongoTask.__subclasses__()}

        ke_data = KEParser(self.input().open('r'), file_path=self.input().path, schema_file=self.keemu_schema_file)

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
        Delete the actual record
        """

        # Delete from MongoDB
        collection.remove({'_id': irn})

if __name__ == "__main__":
    luigi.run(main_task_cls=MongoDeleteTask)