#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.tasks.mongo import MongoTask

class TaxonomyMongoTask(MongoTask):
    """
    Import KE Taxonomy Export file into MongoDB
    """

    module = 'etaxonomy'

    def process(self, data):

        # Currently accepted isn't required so None == Unknown
        try:
            if data['ClaCurrentlyAccepted'] == "Unknown":
                data['ClaCurrentlyAccepted'] = None
        except KeyError:
            pass

        super(TaxonomyMongoTask, self).process(data)