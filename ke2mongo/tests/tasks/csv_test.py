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
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tests.tasks.mongo_test import MongoTestTask

class CSVTestTask(CSVTask):
    """
    Class for exporting exporting KE Mongo data to CSV
    This requires all mongo files have been imported into  Mongo DB
    """
    # Date to process
    mongo_db = 'test'
    collection_name = 'test'
    # date = None
    columns = [
        ('_id', '_id', 'int32'),
        ('SummaryData', 'SummaryData', 'string:100')
    ]
    query = {}

    def requires(self):
        yield MongoTestTask()

    def process_dataframe(self, m, df):
        print df['SummaryData']
        return df

