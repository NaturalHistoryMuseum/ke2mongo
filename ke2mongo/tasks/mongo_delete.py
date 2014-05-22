#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.tasks.mongo import MongoTask

# TODO: Delete
# TODO: Running all. What's the process - get

class MongoDeleteTask(MongoTask):
    """
    Import KE Taxonomy Export file into MongoDB
    """

    module = 'etaxonomy'

    def process(self, data):

        # http://luigi.readthedocs.org/en/latest/luigi_patterns.html
        pass