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
import codecs
import gzip

def encoding_error(e):
    print '------- ERROR -------'
    print e.encoding
    print e.start
    print e.object
    print '------- ERROR -------'
    return (u'-', e.start + 1)

class KEFileTask(luigi.ExternalTask):

    """
    Task wrapper around KE File target
    """
    # After main run:
    # TODO: Email errors
    module = luigi.Parameter()
    file_extension = luigi.Parameter()
    export_dir = luigi.Parameter()
    date = luigi.IntParameter(default=None)

    def output(self):
        return KEFileTarget(self.export_dir, self.module, self.date, self.file_extension)


class KEFileTarget(luigi.LocalTarget):

    file_name = None
    is_tmp = False

    def __init__(self, export_dir, module, date, file_extension):
        """
        Override LocalTarget init to set path and format based on module and settings
        """
        self.export_dir = export_dir
        self.module = module
        self.date = date
        self.file_extension = file_extension
        path, self.file_name, self.format = self.get_file()
        super(KEFileTarget, self).__init__(path, self.format)

    def get_file(self):
        """
        Loop through file and file.gz and return the one that exists
        If neither exists, raise an Exception
        """

        file_name_parts = [self.module, self.file_extension]
        if self.date:
            file_name_parts.append(str(self.date))

        file_name = '.'.join(file_name_parts)

        # List of candidate files and types to try
        candidate_files = [
            (file_name, None),
            ('%s.gz' % file_name, Gzip)
        ]

        for candidate_file, format in candidate_files:
            path = os.path.join(self.export_dir, candidate_file)
            if os.path.exists(path):
                return path, candidate_file, format

        # If the file doesn't exist we want to raise an Exception
        # If a file doesn't exist it hasn't been included in the export and needs to be investigated
        raise IOError('Export files could not be found: Tried: %s %s.gz' % (os.path.join(self.export_dir, file_name), os.path.join(self.export_dir, file_name)))