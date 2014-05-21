#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


from ke2mongo.tasks.dataset import DatasetTask
from random import randint

class CollectionDatasetTask(DatasetTask):
    """
    Class extending
    """
    package = {
        'name': u'nhm2-collection234',
        'notes': u'The Natural History Museum\'s collection',
        'title': "Collection",
        'author': None,
        'author_email': None,
        'license_id': u'other-open',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    # Define the collection name - for other datasets this just uses the specimen collection
    # But we use a reference to the collection built by the aggregation query
    collection_name = 'ecatalogue'