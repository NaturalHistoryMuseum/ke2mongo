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
import re
import luigi
from ke2mongo.log import log
from luigi import scheduler, worker
from luigi.interface import setup_interface_logging
from pymongo import MongoClient
from collections import OrderedDict
from ke2mongo import config
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.mongo_collection_index import MongoCollectionIndexTask
from ke2mongo.tasks.mongo_site import MongoSiteTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.mongo import MongoTarget
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

    bulks_tasks = [MongoCollectionIndexTask, MongoCatalogueTask, MongoTaxonomyTask, MongoMultimediaTask, MongoSiteTask, MongoDeleteTask]


    def requires(self):
        for task in self.bulks_tasks:
            yield task(self.date)

        # yield MongoCollectionIndexTask(self.date), MongoCatalogueTask(self.date), MongoTaxonomyTask(self.date), MongoMultimediaTask(self.date), MongoSiteTask(self.date), MongoDeleteTask(self.date)


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

    # Get a list of all files processed
    mongo_db = config.get('mongo', 'database')
    cursor = MongoClient()[mongo_db][MongoTarget.marker_collection_name].find()

    re_update_id = re.compile('(Mongo[a-zA-Z]+)\(date=([0-9]+)\)')

    # OrderedDict to store all of the update classes
    updates = OrderedDict()

    for record in cursor:
        result = re_update_id.match(record['update_id'])
        if result:
            # NB: Not doing anything with class names
            update_cls = result.group(1)
            update_date = int(result.group(2))
            try:
                updates[update_date].append(update_cls)
            except KeyError:
                updates[update_date] = [update_cls]

    # Make sure the updates have all mongo classes
    bulk_tasks = [task.__name__ for task in MongoBulkTask.bulks_tasks]

    for date, update_tasks in updates.iteritems():
        # Assert that for every date we have all the bulk tasks
        assert list(set(bulk_tasks) - set(update_tasks)) == [], 'There are missing mongo tasks for date %s' % date

    # Get a list of all export files to process
    export_dates = [d for d in get_export_file_dates() if d not in updates.keys()]

    # We do not want to bulk process the most recent
    export_dates.pop()

    # Run setup_interface_logging to ensure luigi commands
    setup_interface_logging()

    sch = scheduler.CentralPlannerScheduler()

    for export_date in export_dates:
        w = BulkWorker(scheduler=sch)
        log.info('Processing date %s', export_date)
        w.add(MongoBulkTask(date=export_date))
        w.run()
        w.stop()

if __name__ == "__main__":
    main()
