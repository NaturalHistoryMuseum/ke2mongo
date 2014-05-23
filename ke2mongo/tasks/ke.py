#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi.postgres
from luigi.format import Gzip
from ke2mongo import config


class KEFileTask(luigi.ExternalTask):

    """
    Task wrapper around KE File target
    """

    # After main run:
    # TODO: Email errors

    module = luigi.Parameter()
    date = luigi.IntParameter(default=None)

    def output(self):
        return KEFileTarget(self.module, self.date)


class KEFileTarget(luigi.LocalTarget):

    export_dir = config.get('keemu', 'export_dir')
    file_name = None
    is_tmp = False

    def __init__(self, module, date, file_extension='export'):
        """
        Override LocalTarget init to set path and format based on module and settings
        """

        path, self.file_name, self.format = self.get_file(module, date, file_extension)

        super(KEFileTarget, self).__init__(path, self.format)

    def get_file(self, module, date, file_extension):
        """
        Loop through file and file.gz and return the one that exists
        If neither exists, raise an Exception
        """

        file_name = '.'.join([module, file_extension, str(date)])

        # List of candidate files and types to try
        candidate_files = [
            (file_name, None),
            ('%s.gz' % file_name, Gzip)
        ]

        for candidate_file, format in candidate_files:
            path = os.path.join(self.export_dir, candidate_file)
            if os.path.isfile(path):
                return path, candidate_file, format

        # If the file doesn't exist we want to raise an Exception
        # If a file doesn't exist it hasn't been included in the export and needs to be investigated
        raise IOError('Export files %s/%s.gz do not exist in directory %s.' % (file_name, file_name, self.export_dir))