#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Add to crontab to run:

# * * * * * /usr/lib/import/bin/python /usr/lib/import/src/ke2mongo/ke2mongo/tasks/cron.py >> /var/log/crontab/ke2mongo.log 2>&1

If running manually, you may want to run without rebuilding the solr specimen index. You can use:

python run.py -l --no-index

"""

import sys
import getopt
import luigi
from ke2mongo import config
from ke2mongo.tasks.specimen import SpecimenDatasetAPITask
from ke2mongo.tasks.indexlot import IndexLotDatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetAPITask
from ke2mongo.tasks.unpublish import UnpublishTask

from ke2mongo.tasks.main import MainTask
from ke2mongo.lib.file import get_export_file_dates
from ke2mongo.lib.mongo import mongo_get_update_markers

def get_export_file_date():
    """
    Get the oldest export file date that hasn't run
    :return:
    """

    update_markers = mongo_get_update_markers()
    completed_dates = []

    # Check all tasks for a particular date have run correctly
    # If they have, add date to completed date
    for date, tasks in update_markers.items():
        for task in MainTask.tasks:
            if task.task_family not in tasks:
                break
            completed_dates.append(date)

    # Loop through all available export file dates, and return the
    # first one we don't have an update marker for
    export_file_dates = get_export_file_dates()

    for export_file_date in export_file_dates:
        if export_file_date not in completed_dates:
            return export_file_date


def main(argv):
    export_file_date = get_export_file_date()
    # Add local scheduler option so we can run manually when needed
    local_scheduler = False
    # And if we don't want to rebuild the indexes after run
    no_index = False

    opts, args = getopt.getopt(argv, "ln", ["local-scheduler", "no-index"])
    for opt, arg in opts:
        if opt in ("-l", "--local-scheduler"):
            local_scheduler = True
        if opt in ("-n", "--no-index"):
            no_index = True

    if export_file_date:
        params = ['--date', str(export_file_date)]

        if no_index:
            params.append('--no-index')

        luigi.run(params, main_task_cls=MainTask, local_scheduler=local_scheduler)

if __name__ == "__main__":
    main(sys.argv[1:])
