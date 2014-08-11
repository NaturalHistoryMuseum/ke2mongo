#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MongoCollectionIndexTask --local-scheduler --date 20140731

"""

from ke2mongo.tasks.mongo import MongoTask

class MongoCollectionIndexTask(MongoTask):
    """
    Import Collection Index Export file into MongoDB
    """
    module = 'ecollectionindex'

