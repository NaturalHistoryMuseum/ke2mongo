#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi.postgres
from ke2mongo import config
from ke2mongo.targets.ke import KEFileTarget


class KEFileTask(luigi.ExternalTask):

    """
    Task wrapper around KE File target
    """
    # After main run:
    # TODO: Email errors
    module = luigi.Parameter()
    file_extension = luigi.Parameter()
    date = luigi.IntParameter(default=None)

    def output(self):
        export_dir = config.get('keemu', 'export_dir')
        return KEFileTarget(export_dir, self.module, self.date, self.file_extension)