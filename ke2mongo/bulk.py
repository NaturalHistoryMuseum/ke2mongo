#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Running bulk does not delete anything from CKAN
It's intended to be run to bulk load everything into a fresh CKAN instance

"""
from ke2mongo import config
from ke2mongo.log import log
from luigi import scheduler, worker
from luigi.interface import setup_interface_logging
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_collection_event import MongoCollectionEventTask
from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.unpublish import UnpublishTask
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
    bulk_tasks = [
        MongoCollectionIndexTask,
        MongoCollectionEventTask,
        MongoCatalogueTask,
        MongoTaxonomyTask,
        MongoMultimediaTask,
        MongoSiteTask,
        UnpublishTask,
        MongoDeleteTask
    ]

    def _get_task_names(tasks):
        """
        We need to initiate and get the family name, not just the class name
        MongoDeleteTask => DeleteTask
        @param tasks:
        @return:
        """
        return [unicode(task(date=0).task_family) for task in tasks]

    full_export_date = int(config.get('keemu', 'full_export_date'))

    for date, update_marker in update_markers.iteritems():

        #  If this is the fll export date, MongoDeleteTask is not required
        if full_export_date and date == full_export_date:
            bulk_task_copy = list(bulk_tasks)
            bulk_task_copy.remove(MongoDeleteTask)
            bulk_task_names = _get_task_names(bulk_task_copy)
        else:
            bulk_task_names = _get_task_names(bulk_tasks)

        # Assert that for every date we have all the bulk tasks
        missing_tasks = list(set(bulk_task_names) - set(update_marker))
        assert missing_tasks == [], 'There are missing mongo tasks for date %s: %s' % (date, missing_tasks)

    # Get a list of all export files to process
    export_dates = [d for d in get_export_file_dates() if d not in update_markers.keys()]

    # Run setup_interface_logging to ensure luigi commands
    setup_interface_logging()

    sch = scheduler.CentralPlannerScheduler()

    w = BulkWorker(scheduler=sch)

    for export_date in export_dates:

        log.info('Processing date %s', export_date)
        # We only need to call the mongo delete task, as all other tasks are a requirement
        # NB: This doesn't delete anything from CKAN - if that's needed change this to DeleteTask
        w.add(MongoDeleteTask(date=export_date, force=True))
        w.run()
        w.stop()

if __name__ == "__main__":
    main()