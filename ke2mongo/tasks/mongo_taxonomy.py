#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.


 python tasks/mongo_taxonomy.py --local-scheduler --date 20150604

Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from ke2mongo.tasks.mongo import MongoTask

class MongoTaxonomyTask(MongoTask):
    """
    Import KE Taxonomy Export file into MongoDB
    """

    module = 'etaxonomy'

    def process_record(self, data):

        # Currently accepted isn't required so None == Unknown
        try:
            if data['ClaCurrentlyAccepted'] == "Unknown":
                data['ClaCurrentlyAccepted'] = None
        except KeyError:
            pass

        return super(MongoTaxonomyTask, self).process_record(data)


if __name__ == "__main__":
    luigi.run(main_task_cls=MongoTaxonomyTask)