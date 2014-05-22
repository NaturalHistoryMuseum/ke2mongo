#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi
import random
import time
from monary import Monary
import numpy as np
import pandas as pd
from ke2mongo.log import log
from ke2mongo.tasks.collection import CollectionDatasetTask
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tasks import ARTEFACT_TYPE

class ArtefactDatasetTask(CollectionDatasetTask):
    """
    Class for exporting exporting IndexLots data to CSV
    """
    name = 'Artefacts'
    description = 'Museum artefacts'
    format = 'csv'

    package = {
        'name': u'nhm-artefacts',
        'notes': u'Artefacts from The Natural History Museum',
        'title': "Artefacts",
        'author': None,
        'author_email': None,
        'license_id': u'other-open',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    columns = [
        ('_id', '_id', 'int32'),
        ('ArtName', 'name', 'string:100'),
        ('ArtKind', 'kind', 'string:100'),
        ('PalArtDescription', 'description', 'string:100'),
        ('MulMultiMediaRef', 'multimedia', 'string:100')
    ]

    query = {"ColRecordType": ARTEFACT_TYPE}
