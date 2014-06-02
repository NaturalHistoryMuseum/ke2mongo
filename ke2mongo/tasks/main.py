#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MainTask --local-scheduler --date 20140123

"""

import luigi
from ke2mongo.tasks.specimen import SpecimenDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask
from ke2mongo.tasks.artefact import ArtefactDatasetTask


class MainTask(luigi.Task):
    """
    Main controller task - runs the three dataset tasks
    """

    date = luigi.IntParameter()

    def requires(self):
        yield ArtefactDatasetTask(self.date), SpecimenDatasetTask(self.date), IndexLotDatasetTask(self.date)