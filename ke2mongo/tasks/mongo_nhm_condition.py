#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'mattw2' on 2014-11-27.
Copyright (c) 2014 'mattw2'. All rights reserved.

python run.py MongoNHMConditionTask --local-scheduler --date 20141127

"""

from ke2mongo.tasks.mongo import MongoTask

class MongoNHMConditionTask(MongoTask):
    """
    Import NHM Condition Export file into MongoDB
    """
    module = 'enhmcondition'

