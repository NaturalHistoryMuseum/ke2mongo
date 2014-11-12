#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ConfigParser import NoOptionError
from ke2mongo import config

def get_export_file_dates():
    """
    Gets all the dates of outstanding files
    @return: list of dates
    """

    export_dir = config.get('keemu', 'export_dir')

    try:
        full_export_date = int(config.get('keemu', 'full_export_date'))
    except NoOptionError:
        full_export_date = None


    files = [f for f in os.listdir(export_dir) if os.path.isfile(os.path.join(export_dir,f))]

    # Use a set so we don't have duplicate dates
    dates = set()

    for f in files:

        # So this will work with both .gz and not compressed files
        f = f.replace('.gz', '')

        try:
            # Extract the date from the file name
            _, _, date = f.split('.')
        except ValueError:
            # file not in the correct format - hidden directory etc.,
            pass
        else:

            date = int(date)

            # If we have full export date (the data the last full dump was produced)
            # we only want dates after the last full dump - so skip prior dates
            if full_export_date and date < full_export_date:
                continue

            dates.add(date)

    # Make sure they are in the right order and convert to list
    dates = sorted(list(dates))

    return dates