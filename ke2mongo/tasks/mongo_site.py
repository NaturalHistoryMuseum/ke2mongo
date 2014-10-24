#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MongoSiteTask --local-scheduler --date 20140814

"""

import luigi
from ke2mongo.tasks.mongo import MongoTask

class MongoSiteTask(MongoTask):
    """
    Import Collection Index Export file into MongoDB
    """
    module = 'esites'

if __name__ == "__main__":
    luigi.run(main_task_cls=MongoSiteTask)