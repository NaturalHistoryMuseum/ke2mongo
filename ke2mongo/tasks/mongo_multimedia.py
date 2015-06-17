#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

 python tasks/mongo_multimedia.py --local-scheduler --date 20150115

"""

import luigi
from ke2mongo.tasks.mongo import MongoTask

class MongoMultimediaTask(MongoTask):
    """
    Import Multimedia Export file into MongoDB
    """
    module = 'emultimedia'

    def process_record(self, data):
        # Add embargoed date = 0 so we don't have to query against field exists (doesn't use the index)
        if not 'NhmSecEmbargoDate' in data:
            data['NhmSecEmbargoDate'] = 0

        # As above - make field indexable
        if not 'GenDigitalMediaId' in data:
            data['GenDigitalMediaId'] = 0

        return super(MongoMultimediaTask, self).process_record(data)

    def on_success(self):
        """
        On completion, add mime format index
        http://www.nhm.ac.uk/emu-classes/class.EMuMedia.php only works with jpeg + jp2 so we need to filter images
        @return: None
        """
        self.collection = self.get_collection()
        # Need to filter on web publishable
        self.collection.ensure_index('AdmPublishWebNoPasswordFlag')
        # And embargo date
        self.collection.ensure_index('NhmSecEmbargoDate')
        # Add MAM GUID field
        self.collection.ensure_index('GenDigitalMediaId')


if __name__ == "__main__":
    luigi.run(main_task_cls=MongoMultimediaTask)