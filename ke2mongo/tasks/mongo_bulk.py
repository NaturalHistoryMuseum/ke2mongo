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

import luigi
from ke2mongo.log import log
from luigi import scheduler, worker
from luigi.interface import setup_interface_logging
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask

from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.delete import DeleteTask
from ke2mongo.lib.file import get_export_file_dates



class MongoBulkTask(luigi.Task):
    """
    This task requires all Mongo Tasks, and processes all of them for a particular date
    Therefore there will be one point of failure

    The main CSV task will fail if there are multiple ke emu export files
    In which case, this task needs to be run with the command:

    python bulk.py
    """

    date = luigi.IntParameter()

    def requires(self):
        yield MongoCollectionIndexTask(self.date), MongoCatalogueTask(self.date), MongoTaxonomyTask(self.date), MongoMultimediaTask(self.date), MongoDeleteTask(self.date)


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

    # Get a list of all export files to process
    export_dates = get_export_file_dates()

    # Run setup_interface_logging to ensure luigi commands
    setup_interface_logging()

    # We do not want to bulk process the most recent
    export_dates.pop()

    sch = scheduler.CentralPlannerScheduler()

    for export_date in export_dates:
        w = BulkWorker(scheduler=sch)
        log.info('Processing date %s', export_date)
        w.add(MongoBulkTask(date=export_date))
        w.run()
        w.stop()

if __name__ == "__main__":
    main()
