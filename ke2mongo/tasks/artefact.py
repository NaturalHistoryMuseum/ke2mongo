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
from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tasks import ARTEFACT_TYPE, MULTIMEDIA_URL


class ArtefactCSVTask(CSVTask):

    columns = [
        ('_id', '_id', 'int32'),
        ('ArtName', 'name', 'string:100'),
        ('ArtKind', 'kind', 'string:100'),
        ('PalArtDescription', 'description', 'string:100'),
        ('MulMultiMediaRef', 'multimedia', 'string:100')
    ]

    query = {"ColRecordType": ARTEFACT_TYPE}

    def process_dataframe(self, m, df):
        """
        Process the dataframe, converting image IRNs to URIs
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """
        # And update images to URLs
        df['multimedia'] = df['multimedia'].apply(lambda x: '; '.join(MULTIMEDIA_URL % z.lstrip() for z in x.split(';') if z))

        return df


class ArtefactDatasetTask(DatasetTask):
    """
    Class for exporting exporting IndexLots data to CSV
    """
    name = 'Artefacts'
    description = 'Museum artefacts'
    format = 'csv'

    package = {
        'name': u'museum-artefacts',
        'notes': u'Cultural and historical artefacts from The Natural History Museum',
        'title': "Artefacts",
        'author': 'Natural History Museum',
        'author_email': None,
        'license_id': u'occ-by',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    full_text_blacklist = [
        'multimedia'
    ]

    csv_class = ArtefactCSVTask


