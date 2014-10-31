#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo import config

PARENT_TYPES = [
    'Bird Group Parent',
    'Mammal Group Parent',
]
# http://www.nhm.ac.uk/emu-classes/class.EMuMedia.php only supports jp2 and jpeg - not gif, tiff etc.,
MULTIMEDIA_FORMATS = ['jp2', 'jpeg']

DATE_FORMAT = 'YYYY-MM-DD'

RECORD_STATUS = 'Active'

COLLECTION_DATASET = {
    'name': 'nhm-specimens-test',
    'notes': u'The Natural History Museum\'s collection',
    'title': "NHM Collection",
    'author': 'Natural History Museum',
    'license_id': u'cc-by',
    'resources': [],
    'dataset_type': 'Specimen',
    'spatial': '{"type":"Polygon","coordinates":[[[-180,82],[180,82],[180,-82],[-180,-82],[-180,82]]]}',
    'owner_org': config.get('ckan', 'owner_org')
}

