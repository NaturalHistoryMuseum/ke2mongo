#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'mattw2' on 2014-09-30.
Copyright (c) 2014 'mattw2'. All rights reserved.

python run.py MongoCollectionEventsTask --local-scheduler --date 20140731

"""

from ke2mongo.tasks.mongo import MongoTask

class MongoCollectionEventsTask(MongoTask):
    """
    Import Collection Events Export file into MongoDB
    """
    module = 'ecollectionevents'

