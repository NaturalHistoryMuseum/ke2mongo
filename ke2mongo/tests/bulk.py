#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
import unittest
from nose.tools import assert_equal
from nose.tools import assert_not_equal
from nose.tools import assert_raises
from nose.tools import raises
from ke2mongo import config
from ke2mongo.lib.ckan import call_action
import psycopg2
from ke2mongo.bulk import main as bulk_run
from ke2mongo import config
import random
import shutil
import tempfile

class TestBulk(unittest.TestCase):

    export_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'bulk')

    archive_dir = '/vagrant/tmp/ke2mongo'

    files = []

    @classmethod
    def setup_class(cls):

        # Remove and recreate export dir
        try:
            shutil.rmtree(cls.export_dir)
        except OSError:
            pass

        os.mkdir(cls.export_dir)

        try:
            os.mkdir(cls.archive_dir)
        except OSError:
            pass

        # Create dummy test files
        file_names = [
            'eaudit.deleted-export',
            'ecatalogue.export',
            'emultimedia.export',
            'etaxonomy.export'
        ]

        # Create empty files

        dates = []

        for i in range(5):
            dates.append(random.getrandbits(64))

        dates.sort()

        for date in dates:

            # Create a list of files and the batch they're part of
            files = []

            for file_name in file_names:
                file_name += '.%s' % date
                f = os.path.join(cls.export_dir, file_name)
                files.append(file_name)
                # Create the file in the data bulk directory
                open(f, 'a').close()

            cls.files.append(files)

    def test_order(self):

        config.set('keemu', 'export_dir', self.export_dir)
        config.set('keemu', 'archive_dir', self.archive_dir)

        print self.files

        # Run the bulk import
        bulk_run()

        existing_files = self.files.pop()
        for existing_file in existing_files:
            f = os.path.join(self.export_dir, existing_file)
            assert os.path.isfile(f), 'File %s does not exist' % f

        for deleted_files in self.files:
            for f in deleted_files:

                export_file = os.path.join(self.export_dir, f)
                archive_file = os.path.join(self.archive_dir, f)

                # Make sure the export file has been deleted
                assert not os.path.isfile(export_file), 'File %s should have been deleted' % export_file

                # And the archive file has been created
                assert os.path.isfile(archive_file), 'Archive file %s has not been created' % archive_file
