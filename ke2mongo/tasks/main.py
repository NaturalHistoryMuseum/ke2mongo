#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

If


"""

import luigi
from ke2mongo.tasks.specimen import SpecimenDatasetAPITask
from ke2mongo.tasks.indexlot import IndexLotDatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetAPITask

class MainTask(luigi.Task):
    """
    Convenience function for running all three tasks
    """

    date = luigi.IntParameter()

    # List of all tasks that need to be run
    tasks = [ArtefactDatasetAPITask, IndexLotDatasetAPITask, SpecimenDatasetAPITask]

    def requires(self):

        params = {
            'date': self.date,
        }
        for task in self.tasks:
            yield task(**params)