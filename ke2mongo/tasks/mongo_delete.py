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
from ke2mongo.tasks.delete import DeleteTask


class MongoDeleteTask(DeleteTask):
    """
    Delete records from Mongo DB
    This does not delete the corresponding records from CKAN dataset
    This should only be used prior to rebuilding the entire dataset

    To ensure it's not called in error, it must be called with flag force

    python tasks/mongo_delete.py --date 20140821 --local-scheduler --force

    """

    force = luigi.BooleanParameter(default=False, significant=False)

    # Use the same task family asDeleteTask
    # if DeleteTask has run, we do not want this to run, and vice versa
    task_family = DeleteTask.task_family

    def run(self):

        if not self.force:
            raise Exception('Warning: this class does not delete CKAN records. Use --force to run it.')

        super(MongoDeleteTask, self).run()

    def ckan_delete(self, collection, irn):
        """
        In this class, we do not delete the CKAN record
        """
        pass

if __name__ == "__main__":
    luigi.run(main_task_cls=MongoDeleteTask)