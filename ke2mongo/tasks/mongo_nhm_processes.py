#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'mattw2' on 2014-11-27.
Copyright (c) 2014 'mattw2'. All rights reserved.

python run.py MongoNHMProcessesTask --local-scheduler --date 20141127

"""

from ke2mongo.tasks.mongo import MongoTask

class MongoNHMProcessesTask(MongoTask):
    """
    Import NHM Processes Export file into MongoDB
    """
    module = 'enhmprocesses'

