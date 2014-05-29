#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask

# TEMP
import random, string

def randomword(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))

class CollectionDatasetTask(DatasetTask):
    """
    Base collections dataset class
    Extended by indexlots and collections
    """
    package = {
        'name': u'nhm-collection_%s' % randomword(10),
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