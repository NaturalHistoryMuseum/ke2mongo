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

import sys
import os
import luigi
import logging
from ke2mongo.log import log
from luigi import scheduler, worker
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.main import MainTask
from ke2mongo.lib.file import get_export_file_dates
from luigi.interface import setup_interface_logging


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
        yield MongoDeleteTask(self.date), MongoTaxonomyTask(self.date), MongoMultimediaTask(self.date), MongoBulkCatalogueTask(self.date)


class MongoBulkCatalogueTask(MongoCatalogueTask):
    """
    After importing, MongoCatalogueTask adds indexes and updates child refs.
    We do not want these to run during bulk imports, as it slows it down
    So override the on_success to prevent this
    """
    def on_success(self):
        # Do not pass go, do not collect 200
        pass

    # Update ID


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
    w = BulkWorker(scheduler=sch)

    for export_date in export_dates:
        log.info('Processing date %s', export_date)
        w.add(MongoBulkTask(date=export_date))

    w.run()

    w.stop()

if __name__ == "__main__":
    main()
