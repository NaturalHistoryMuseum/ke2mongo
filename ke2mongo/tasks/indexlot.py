#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""
import luigi
import numpy as np
import pandas as pd
from collections import OrderedDict
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask
from ke2mongo.tasks import COLLECTION_DATASET

class IndexLotDatasetTask(DatasetTask):

    record_type = 'Index Lot'

    # CKAN Dataset params
    package = COLLECTION_DATASET

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'indexlots6',
            'description': 'Index lots',
            'format': 'csv'
        },
        'primary_key': 'Catalogue number'
    }

    columns = [
        ('irn', 'Catalogue number', 'string:100'),
        ('EntIndIndexLotNameRef', '_collection_index_irn', 'int32'),
        ('EntIndMaterial', 'Material', 'bool'),
        ('EntIndType', 'Type', 'bool'),
        ('EntIndMedia', 'Media', 'bool'),
        ('EntIndKindOfMaterial', 'Kind of material', 'string:100'),
        ('EntIndKindOfMedia', 'Kind of media', 'string:100'),

        # Material detail
        ('EntIndCount', 'Material count', 'string:100'),
        ('EntIndSex', 'Material sex', 'string:100'),
        ('EntIndStage', 'Material stage', 'string:100'),
        ('EntIndTypes', 'Material types', 'string:100'),
        ('EntIndPrimaryTypeNo', 'Material primary type no', 'string:100'),
        ('EntIndRemarks', 'Material remarks', 'string:100'),

        # Separate Botany and Entomology
        ('ColDepartment', 'Department', 'string:100'),
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
        ('ColCurrentNameRef', '_current_name_irn', 'int32'),
    ]

    # Additional columns to merge in from the taxonomy collection
    taxonomy_columns = [
        ('_id', '_taxonomy_irn', 'int32'),
        ('ClaScientificNameBuilt', 'Original name', 'string:100'),
        ('ClaKingdom', 'Kingdom', 'string:60'),
        ('ClaPhylum', 'Phylum', 'string:100'),
        ('ClaClass', 'Class', 'string:100'),
        ('ClaOrder', 'Order', 'string:100'),
        ('ClaSuborder', 'Suborder', 'string:100'),
        ('ClaSuperfamily', 'Superfamily', 'string:100'),
        ('ClaFamily', 'Family', 'string:100'),
        ('ClaSubfamily', 'Subfamily', 'string:100'),
        ('ClaGenus', 'Genus', 'string:100'),
        ('ClaSubgenus', 'Subgenus', 'string:100'),
        ('ClaSpecies', 'Species', 'string:100'),
        ('ClaSubspecies', 'Subspecies', 'string:100'),
        ('ClaRank', 'Taxonomic rank', 'string:20'),  # NB: CKAN uses rank internally
    ]

    current_name_columns = [
        ('_id', '_current_name_irn', 'int32'),
        ('ClaScientificNameBuilt', 'Currently accepted name', 'string:100'),
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

        df = super(IndexLotDatasetTask, self).process_dataframe(m, df)

        # Convert booleans to yes / no
        # need to use original columns definition, as
        for (_, field, field_type) in self.columns:
            if field_type == 'bool':
                df[field][df[field] == 'True'] = 'Yes'
                df[field][df[field] == 'False'] = 'No'
                df[field][df[field] == 'N/A'] = ''

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

        # Add current name - same process as the main taxonomy but using _current_name_irn source fields
        current_name_irns = pd.unique(collection_index_df._current_name_irn.values.ravel()).tolist()
        current_name_df = self.get_dataframe(m, 'etaxonomy', self.current_name_columns, current_name_irns, '_current_name_irn')
        collection_index_df = pd.merge(collection_index_df, current_name_df, how='inner', left_on=['_current_name_irn'], right_on=['_current_name_irn'])

        # Merge results into main dataframe
        df = pd.merge(df, collection_index_df, how='outer', left_on=['_collection_index_irn'], right_on=['_collection_index_irn'])

        return df

    def get_output_columns(self):
        """
        Get a list of output columns, with bool converted to string:3 (so can be converted to Yes/No)
        @return:
        """
        return OrderedDict((col[1], 'string:3' if col[2] == 'bool' else col[2]) for col in self.columns + self.taxonomy_columns + self.current_name_columns if self._is_output_field(col[1]))


class IndexLotDatasetCSVTask(IndexLotDatasetTask, DatasetCSVTask):
    pass


class IndexLotDatasetAPITask(IndexLotDatasetTask, DatasetAPITask):
    pass

if __name__ == "__main__":
    luigi.run()