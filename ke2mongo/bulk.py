#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

The main task will fail if there are Bulkple KE EMu exports
See CSVTask.__init__()
This is to ensure that an error in the exports - ie: they're not available on the due date
is investigated

"""
from ke2mongo.log import log
from luigi import scheduler, worker
from luigi.interface import setup_interface_logging
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.mongo_delete import DeleteTask
from ke2mongo.lib.file import get_export_file_dates
from ke2mongo.lib.mongo import mongo_get_update_markers

class BulkException(Exception):
    """
    Batch error exception
    """
    pass

class BulkWorker(worker.Worker):
    """
    Extend the worker class so if there's an error, the whole batch terminates
    """
    def _log_complete_error(self, task):
        super(BulkWorker, self)._log_complete_error(task)
        # If there's an
        raise BulkException

    def _log_unexpected_error(self, task):
        super(BulkWorker, self)._log_unexpected_error(task)
        # If there's an
        raise BulkException


def main():

    update_markers = mongo_get_update_markers()

    # Make sure the updates have all mongo classes
    bulk_tasks = [task.__name__ for task in [MongoCollectionIndexTask, MongoCatalogueTask, MongoTaxonomyTask, MongoMultimediaTask, MongoSiteTask, DeleteTask]]

    for date, update_marker in update_markers.iteritems():
        # Assert that for every date we have all the bulk tasks
        assert list(set(bulk_tasks) - set(update_marker)) == [], 'There are missing mongo tasks for date %s' % date

    # Get a list of all export files to process
    export_dates = [d for d in get_export_file_dates() if d not in update_markers.keys()]

    # We do not want to bulk process the most recent
    export_dates.pop()

    # Run setup_interface_logging to ensure luigi commands
    setup_interface_logging()

    sch = scheduler.CentralPlannerScheduler()

    for export_date in export_dates:
        w = BulkWorker(scheduler=sch)
        log.info('Processing date %s', export_date)
        # We only need to call the delete task, as all other tasks are a requirement
        w.add(DeleteTask(date=export_date))
        w.run()
        w.stop()

if __name__ == "__main__":
    main()
