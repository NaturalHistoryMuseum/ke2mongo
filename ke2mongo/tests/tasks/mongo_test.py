#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.


python test_csv.py MongoTestTask --local-scheduler

"""

import sys
import os
import luigi
from ke2mongo.tasks.mongo import MongoTask

class MongoTestTask(MongoTask):

    module = 'ecatalogue'
    database = 'test'
    collection_name = 'test'

    # Export dir is is the parent directory's data directory
    export_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'data')

    def mark_complete(self):
        # Do not archive the file
        # self.output().touch()
        pass