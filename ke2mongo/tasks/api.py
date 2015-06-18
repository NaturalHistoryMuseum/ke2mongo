#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import ckanapi
from ke2mongo.log import log
from ke2mongo import config
import luigi

class APITask(luigi.Task):
    """
    Base CKAN API Task
    """

    # The name of the ckan instance to use - must match a ckan setting in the config file
    # Currently one of local, dev, or live
    target = luigi.Parameter(default='local')

    # Date to process
    date = luigi.IntParameter()

    full_export_date = config.get('keemu', 'full_export_date')

    def __init__(self, *args, **kwargs):

        # If a date parameter has been passed in, we'll just use that
        # Otherwise, loop through the files and get all dates
        super(APITask, self).__init__(*args, **kwargs)

        # target is local, live etc - so match up with config settings ckan.local
        config_setting = 'ckan.%s' % self.target
        self.remote_ckan = ckanapi.RemoteCKAN(config.get(config_setting, 'site_url'), apikey=config.get(config_setting, 'api_key'))