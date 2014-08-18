#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import numpy as np
import pandas as pd
from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.tasks.specimen import SpecimenDatasetTask
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tasks import INDEX_LOT_TYPE

class IndexLotCSVTask(CSVTask):

    columns = [
        ('_id', '_id', 'int32'),
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

    # query = {"ColRecordType": INDEX_LOT_TYPE, '_id': {'$in': [1132482, 1637891]}}

    query = {"ColRecordType": INDEX_LOT_TYPE}

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

    def csv_output_columns(self):
        """
        Columns to output to CSV - overrideable
        @return: Dictionary field_name : type
        """
        return self._map_csv_columns(self.columns + self.taxonomy_columns)

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

        taxonomy_df = self.get_dataframe(m, 'etaxonomy', self.taxonomy_columns, taxonomy_irns, '_taxonomy_irn')

        # Merge the taxonomy into the collection index dataframe - we need to do this so we can merge into
        # main dataframe keyed by collection index ID
        collection_index_df = pd.merge(collection_index_df, taxonomy_df, how='inner', left_on=['_taxonomy_irn'], right_on=['_taxonomy_irn'])

        # Merge results into main dataframe
        df = pd.merge(df, collection_index_df, how='outer', left_on=['_collection_index_irn'], right_on=['_collection_index_irn'])

        return df

    def get_dataframe(self, m, collection, columns, irns, key):
        # The query to pre-load all taxonomy objects takes ~96 seconds
        # It is much faster to load taxonomy objects on the fly, for the current block
        # collection_index_irns = pd.unique(df._collection_index_irn.values.ravel()).tolist()

        ke_cols, df_cols, types = zip(*columns)

        assert key in df_cols, 'Merge dataframe key must be present in dataframe columns'

        q = {'_id': {'$in': irns}}

        query = m.query('keemu', collection, q, ke_cols, types)
        df = pd.DataFrame(np.matrix(query).transpose(), columns=df_cols)

        # Convert to int (adding index doesn't speed this up)
        df[key] = df[key].astype('int32')

        return df



class IndexLotDatasetTask(DatasetTask):
    """
    Class for exporting exporting IndexLots data to CSV
    """
    name = 'Indexlots'
    description = 'Entomology Indexlot records'
    format = 'csv'

    # Use the same package details as the Specimen Dataset task
    package = SpecimenDatasetTask.package

    csv_class = IndexLotCSVTask



