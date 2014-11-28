#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'mattw2' on 2014-09-30.
Copyright (c) 2014 'mattw2'. All rights reserved.

python run.py MongoNHMStratigraphyTask --local-scheduler --date 20140731

"""

from ke2mongo.tasks.mongo import MongoTask

class MongoNHMStratigraphyTask(MongoTask):
    """
    Import NHM Stratigraphy Export file into MongoDB
    """
    module = 'enhmstratigraphy'

