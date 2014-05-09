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

    # After main run:
    # TODO: Data param & schedule
    # TODO: Email errors

    module = luigi.Parameter()
    date = luigi.DateParameter(default=None)
    file_name = luigi.Parameter(default='export')
    export_dir = luigi.Parameter(default=config.get('keemu', 'export_dir'))

    def output(self):

        import_file_path = self.get_file_path()
        file_format = Gzip if '.gz' in import_file_path else None
        target = luigi.LocalTarget(import_file_path, format=file_format)
        if not target.exists():
            raise Exception('Export file %s for %s does not exist (Path: %s).' % (self.module, self.date, target.path))
        return target

    def get_file_path(self):

        file_name = [self.module, self.file_name]
        if self.date:
            file_name.append(self.date.strftime('%Y%m%d'))

        path = os.path.join(self.export_dir, '.'.join(file_name))

        #  If we don't have a compressed file, try an uncompressed one
        if not os.path.isfile(path):
            path += '.gz'

        return path