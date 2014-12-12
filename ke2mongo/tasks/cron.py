#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Add to crontab to run:

# * * * * * /usr/lib/import/bin/python /usr/lib/import/src/ke2mongo/ke2mongo/tasks/cron.py >> /var/log/crontab/ke2mongo.log 2>&1

"""

import luigi
from ke2mongo.tasks.specimen import SpecimenDatasetAPITask
from ke2mongo.tasks.indexlot import IndexLotDatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetAPITask
from ke2mongo.lib.file import get_export_file_dates
from ke2mongo.lib.mongo import mongo_get_update_markers

class CronTask(luigi.Task):
    """
    Main controller task - runs the three dataset ckan API tasks on cron
    """

    date = luigi.IntParameter()

    def requires(self):

        raise Exception('HEY')

        # Run all the API tasks
        yield ArtefactDatasetAPITask(self.date), IndexLotDatasetAPITask(self.date), SpecimenDatasetAPITask(self.date)


if __name__ == "__main__":

    update_markers = mongo_get_update_markers()

    # Get most recent export file date
    last_export_date = get_export_file_dates()[-1]

    # Has this already run
    if last_export_date in update_markers.keys():
        # raise Exception('Most recent file date %s has already been processed. Has the export failed?' % last_export_date)
        pass



    # TEMP: Remove
    last_export_date = 20141204

    luigi.run(['--date', str(last_export_date)], main_task_cls=CronTask)