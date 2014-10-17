#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo import config
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask

class ArtefactDatasetTask(DatasetTask):

    # CKAN Dataset params
    package = {
        'name': 'artefacts2',
        'notes': u'Cultural and historical artefacts from The Natural History Museum',
        'title': "Artefacts",
        'author': 'Natural History Museum',
        'license_id': u'cc-by',
        'resources': [],
        'dataset_type': 'Cultural artefacts',
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Artefacts',
            'description': 'Museum artefacts',
            'format': 'csv'
        },
        'primary_key': 'Identifier'
    }

    columns = [
        ('_id', '_id', 'int32'),
        ('irn', 'Identifier', 'string:100'),
        ('ArtName', 'Name', 'string:100'),
        ('ArtKind', 'Kind', 'string:100'),
        ('PalArtDescription', 'Description', 'string:100'),
        ('IdeCurrentScientificName', 'Scientific name', 'string:100'),
        ('MulMultiMediaRef', 'Multimedia', 'string:100')
    ]

    record_type = 'Artefact'

    def process_dataframe(self, m, df):
        """
        Process the dataframe, converting image IRNs to URIs
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """
        # And update images to URLs

        self.ensure_multimedia(m, df, 'Multimedia')
        return df


class ArtefactDatasetCSVTask(ArtefactDatasetTask, DatasetCSVTask):
    pass


class ArtefactDatasetAPITask(ArtefactDatasetTask, DatasetAPITask):
    pass