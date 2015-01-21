#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python tasks/artefact.py ArtefactDatasetAPITask

"""

import luigi
from ke2mongo import config
from ke2mongo.tasks import DATASET_LICENCE, DATASET_AUTHOR, DATASET_TYPE
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask


class ArtefactDatasetTask(DatasetTask):

    # CKAN Dataset params
    package = {
        'name': 'collection-artefacts-json-2',
        'notes': u'Cultural and historical artefacts from The Natural History Museum',
        'title': "Artefacts",
        'author': DATASET_AUTHOR,
        'license_id': DATASET_LICENCE,
        'resources': [],
        'dataset_type': DATASET_TYPE,
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Artefacts',
            'description': 'Museum artefacts',
            'format': 'csv'
        },
        'primary_key': 'GUID'
    }

    columns = [
        ('ecatalogue.AdmGUIDPreferredValue', 'GUID', 'uuid'),
        ('ecatalogue.ArtName', 'Name', 'string:100'),
        ('ecatalogue.ArtKind', 'Kind', 'string:100'),
        ('ecatalogue.PalArtDescription', 'Description', 'string:100'),
        ('ecatalogue.IdeCurrentScientificName', 'Scientific name', 'string:100'),
        ('ecatalogue.MulMultiMediaRef', 'Multimedia', 'json')
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

        df = super(ArtefactDatasetTask, self).process_dataframe(m, df)
        self.ensure_multimedia(df, 'Multimedia')
        return df


class ArtefactDatasetCSVTask(ArtefactDatasetTask, DatasetCSVTask):
    pass


class ArtefactDatasetAPITask(ArtefactDatasetTask, DatasetAPITask):
    pass


if __name__ == "__main__":
    luigi.run()