#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

"""

from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.tests.tasks.csv_test import CSVTestTask


class SpecimenDatasetTestTask(DatasetTask):
    """
    Class for creating specimens DwC dataset
    """
    name = 'test_dataset_7'
    description = 'Test'
    format = 'dwc'
    date = None

    package = {
        'name': u'test2_5',
        'notes': u'Test',
        'title': "Test",
        'author': 'NHM',
        'license_id': u'other-open',
        'resources': [],
    }

    csv_class = CSVTestTask

