#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MainTask --local-scheduler --date 20140123

"""
import os
import luigi
import sys
from ke2mongo import config
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.dwc import DarwinCoreDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask
from ke2mongo.tasks.artefact import ArtefactDatasetTask

class MainTask(luigi.Task):
    """
    Main controller task
    Loops through all files in the export directory to retrieve date parameter
    Of all outstanding files (files are moved on completion )
    And then calls the child tasks
    """

    date = luigi.IntParameter(default=None)
    export_dir = config.get('keemu', 'export_dir')
    dates = []

    def get_dates(self):
        """
        Gets all the dates of outstanding files
        @return: list of dates
        """

        files = [f for f in os.listdir(self.export_dir) if os.path.isfile(os.path.join(self.export_dir,f))]

        # Use a set so we don't have duplicate dates
        dates = set()

        for f in files:

            try:
                # Extract the date from the file name
                _, _, date, _ = f.split('.')
            except ValueError:
                # file not in the correct format - hidden directory etc.,
                pass
            else:
                dates.add(int(date))

        return dates

    def __init__(self, *args, **kwargs):

        # If a date parameter has been passed in, we'll just use that
        # Otherwise, loop through the files and get all dates
        super(MainTask, self).__init__(*args, **kwargs)

        if self.date:
            self.dates = [self.date]
        else:
            self.dates = self.get_dates()

    def requires(self):

        # Loop through all dates and process the mongo tasks
        for date in self.dates:
            # yield MongoDeleteTask(date), MongoCatalogueTask(date), MongoTaxonomyTask(date)
            yield MongoCatalogueTask(date)

        # After ALL mongo tasks have been processed, build the datasets
        # yield ArtefactDatasetTask(), DarwinCoreDatasetTask(), IndexLotDatasetTask()