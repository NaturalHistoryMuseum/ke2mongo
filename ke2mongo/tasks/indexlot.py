#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import numpy as np
import pandas as pd
from collections import OrderedDict
from ke2mongo import config
from ke2mongo.tasks.dataset import DatasetTask, DatasetToCSVTask, DatasetToCKANTask

class IndexLotDatasetTask(DatasetTask):

    record_type = 'Index Lot'

    columns = [
        ('_id', '_id', 'int32'),
        ('_id', 'catalogue_number', 'int32'),
        ('EntIndIndexLotNameRef', '_collection_index_irn', 'int32'),
        ('EntIndMaterial', 'material', 'bool'),
        ('EntIndType', 'is_type', 'bool'),
        ('EntIndMedia', 'media', 'bool'),
        ('EntIndKindOfMaterial', 'kind_of_material', 'string:100'),
        ('EntIndKindOfMedia', 'kind_of_media', 'string:100'),
        # Material detail
        ('EntIndCount', 'material_count', 'string:100'),
        ('EntIndTypes', 'material_types', 'string:100'),
    ]

    # Additional columns to merge in from the taxonomy collection
    collection_index_columns = [
        ('_id', '_collection_index_irn', 'int32'),
        # BUG FIX BS 140811
        # ColCurrentNameRef Is not being updated correctly - see record 899984
        # ColCurrentNameRef = 964105
        # Not a problem, as indexlots are using ColTaxonomicNameRef for summary data etc.,
        # So ColTaxonomicNameRef is the correct field to use.
        ('ColTaxonomicNameRef', '_taxonomy_irn', 'int32'),
    ]

    # Additional columns to merge in from the taxonomy collection
    taxonomy_columns = [
        ('_id', '_taxonomy_irn', 'int32'),
        ('ClaScientificNameBuilt', 'scientific_name', 'string:100'),
        ('ClaKingdom', 'kingdom', 'string:60'),
        ('ClaPhylum', 'phylum', 'string:100'),
        ('ClaClass', 'class', 'string:100'),
        ('ClaOrder', 'order', 'string:100'),
        ('ClaSuborder', 'suborder', 'string:100'),
        ('ClaSuperfamily', 'superfamily', 'string:100'),
        ('ClaFamily', 'family', 'string:100'),
        ('ClaSubfamily', 'subfamily', 'string:100'),
        ('ClaGenus', 'genus', 'string:100'),
        ('ClaSubgenus', 'subgenus', 'string:100'),
        ('ClaSpecies', 'species', 'string:100'),
        ('ClaSubspecies', 'subspecies', 'string:100'),
        ('ClaRank', 'taxonomic_rank', 'string:10'),  # NB: CKAN uses rank internally
    ]

    def process_dataframe(self, m, df):
        """
        Process the dataframe, adding in the taxonomy fields
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """

        # Try and get taxonomy using the collection index
        # BS: 20140804 - Fix indexlots taxonomy bug
        # When the index lot record's taxonomy is updated (via collection index),
        # the index lot record's EntIndIndexLotTaxonNameLocalRef is not updated with the new taxonomy
        # So we need to use collection index to retrieve the record taxonomy

        collection_index_irns = pd.unique(df._collection_index_irn.values.ravel()).tolist()

        collection_index_df = self.get_dataframe(m, 'ecollectionindex', self.collection_index_columns, collection_index_irns, '_collection_index_irn')

        # And get the taxonomy for these collection
        taxonomy_irns = pd.unique(collection_index_df._taxonomy_irn.values.ravel()).tolist()

        # The query to pre-load all taxonomy objects takes ~96 seconds
        # It is much faster to load taxonomy objects on the fly, for the current block
        # collection_index_irns = pd.unique(df._collection_index_irn.values.ravel()).tolist()
        taxonomy_df = self.get_dataframe(m, 'etaxonomy', self.taxonomy_columns, taxonomy_irns, '_taxonomy_irn')

        # Merge the taxonomy into the collection index dataframe - we need to do this so we can merge into
        # main dataframe keyed by collection index ID
        collection_index_df = pd.merge(collection_index_df, taxonomy_df, how='inner', left_on=['_taxonomy_irn'], right_on=['_taxonomy_irn'])

        # Merge results into main dataframe
        df = pd.merge(df, collection_index_df, how='outer', left_on=['_collection_index_irn'], right_on=['_collection_index_irn'])

        return df

    def get_output_columns(self):
        return OrderedDict((col[1], col[2]) for col in self.columns + self.taxonomy_columns if self._is_output_field(col[1]))


class IndexLotDatasetToCSVTask(IndexLotDatasetTask, DatasetToCSVTask):
    pass


class IndexLotDatasetToCKANTask(IndexLotDatasetTask, DatasetToCKANTask):

    package = {
        'name': 'specimens-12348',
        'notes': u'The Natural History Museum\'s collection',
        'title': "NHM Collection",
        'author': 'Natural History Museum',
        'license_id': u'cc-by',
        'resources': [],
        'dataset_type': 'Specimen',
        'spatial': '{"type":"Polygon","coordinates":[[[-180,82],[180,82],[180,-82],[-180,-82],[-180,82]]]}',
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Index lots',
            'description': 'Index lots',
            'format': 'csv'
        },
    }

    primary_key = 'catalogue_number'



