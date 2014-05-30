#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi
import logging
from ke2mongo.log import log
from luigi import scheduler, worker
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.main import MongoDeleteTask
from ke2mongo.lib.file import get_export_file_dates
from luigi.interface import setup_interface_logging


class MongoBatchTask(luigi.Task):
#     """
#     This task acts as a chained task controller, ensuring old keemu files are imported into mongo db in the right order
#     And stops at point of failure - ie: if there's one missing file
#
#     The main CSV task will fail if there are multiple ke emu export files
#     In which case, this task needs to be run with the command:
#     python bulk.py MongoBacklogTask --local-scheduler
#     """
#
    date = luigi.IntParameter()
#
#     def __init__(self, *args, **kwargs):
#         """
#         Override init to retrieve the resource id
#         @param args:
#         @param kwargs:
#         @return:
#         """
#
#         super(MongoBacklogTask, self).__init__(*args, **kwargs)
#
#         self.file_export_dates = get_export_file_dates()
#
#         # If we haven't received a date, use the first one
#         # IMPORTANT: Luigi tasks are processed in reverse order, so we need to add them in order from start to finish
#         self.current_date = self.date if self.date else self.file_export_dates[-1]
#
#
#
#         # We then get the index for this date
#         current_date_index = self.file_export_dates.index(self.current_date)
#
#         # Which allows us to get the next date
#         try:
#             self.prev_date = self.file_export_dates[current_date_index-1:current_date_index].pop()
#         except IndexError:
#             # If pop fails, there are no more previous dates to iterate to
#             self.prev_date = None
#
#         print 'INIT', self.current_date
#
    def requires(self):
        yield MongoDeleteTask(self.date), MongoTaxonomyTask(self.date)

#         if self.prev_date:
#             print '--------------------------------------------'
#             print '-------------------------------', self.prev_date
#             # log.info('Queueing MongoDeleteTask(%s)', self.next_date)
#             print '--------------------------------------------'
#
#             # The order is reversed, so MongoDeleteTask runs first
#             # This is important as the MongoBacklogTask completion check looks for the existence of the file
#             return [MongoBacklogTask(self.prev_date), MongoDeleteTask(self.prev_date)]
#         else:
#             log.info('Queue build. Running tasks.')
#
#     def run(self):
#         print 'RUN', self.current_date
#
#     def complete(self):
#         return False


class BatchException(Exception):
    """
    Batch error exception
    """
    pass

class BatchWorker(worker.Worker):
    """
    Extend the worker class so if there's an exception, the whole batch terminates
    """
    def _log_complete_error(self, task):
        super(BatchWorker, self)._log_complete_error(task)
        # If there's an
        raise BatchException


def main():

    # Get a list of all export files to process
    export_dates = get_export_file_dates()

    setup_interface_logging()

    # We do not want to bulk process the most recent

    last_date = export_dates.pop()

    sch = scheduler.CentralPlannerScheduler()
    w = BatchWorker(scheduler=sch)

    for export_date in export_dates:
        w.add(MongoBatchTask(date=export_date))

    w.run()
    w.stop()


if __name__ == "__main__":
    # luigi.run()
    #
    main()
