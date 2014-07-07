#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.tasks.mongo import MongoTask

class MongoMultimediaTask(MongoTask):
    """
    Import Multimedia Export file into MongoDB
    """
    module = 'emultimedia'

    def on_success(self):
        """
        On completion, add mime type index
        @return: None
        """
        self.collection = self.get_collection()
        self.collection.ensure_index('MulMimeType')
        # Or try MulMimeFormat?

