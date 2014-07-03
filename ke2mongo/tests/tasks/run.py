#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.


python run.py SpecimenDatasetTestTask --local-scheduler
python run.py MongoTestTask --date 1 --local-scheduler

"""

import luigi
from ke2mongo.tests.tasks.csv_test import CSVTestTask
from ke2mongo.tests.tasks.mongo_test import MongoTestTask
from ke2mongo.tests.tasks.specimen_test import SpecimenDatasetTestTask


if __name__ == "__main__":
    luigi.run()
