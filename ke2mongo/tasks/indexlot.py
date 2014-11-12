#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python indexlot.py IndexLotDatasetAPITask --local-scheduler

"""
import luigi
import pandas as pd
from collections import OrderedDict
from ke2mongo import config
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask
from ke2mongo.tasks import DATASET_LICENCE, DATASET_AUTHOR, DATASET_TYPE

class IndexLotDatasetTask(DatasetTask):

    record_type = 'Index Lot'

    # CKAN Dataset params
    package = {
        'name': 'collection-indexlot2',
        'notes': u'Index Lot records from the Natural History Museum\'s collection',
        'title': "Index Lot collection",
        'author': DATASET_AUTHOR,
        'license_id': DATASET_LICENCE,
        'resources': [],
        'dataset_type': DATASET_TYPE,
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Index Lots',
            'description': 'Species level record denoting the presence of a taxon in the Museum collection',
            'format': 'csv'
        },
        'primary_key': 'Catalogue number'
    }

    columns = [
        ('etaxonomy2._id', '_current_name_irn', 'int32'),
        ('etaxonomy2.ClaScientificNameBuilt', 'Currently accepted name', 'string:100'),

        ('etaxonomy._id', '_taxonomy_irn', 'int32'),
        ('etaxonomy.ClaScientificNameBuilt', 'Original name', 'string:100'),
        ('etaxonomy.ClaKingdom', 'Kingdom', 'string:60'),
        ('etaxonomy.ClaPhylum', 'Phylum', 'string:100'),
        ('etaxonomy.ClaClass', 'Class', 'string:100'),
        ('etaxonomy.ClaOrder', 'Order', 'string:100'),
        ('etaxonomy.ClaSuborder', 'Suborder', 'string:100'),
        ('etaxonomy.ClaSuperfamily', 'Superfamily', 'string:100'),
        ('etaxonomy.ClaFamily', 'Family', 'string:100'),
        ('etaxonomy.ClaSubfamily', 'Subfamily', 'string:100'),
        ('etaxonomy.ClaGenus', 'Genus', 'string:100'),
        ('etaxonomy.ClaSubgenus', 'Subgenus', 'string:100'),
        ('etaxonomy.ClaSpecies', 'Species', 'string:100'),
        ('etaxonomy.ClaSubspecies', 'Subspecies', 'string:100'),
        ('etaxonomy.ClaRank', 'Taxonomic rank', 'string:20'),  # NB: CKAN uses rank internally

        ('ecatalogue.irn', 'Catalogue number', 'string:100'),
        ('ecatalogue.EntIndIndexLotNameRef', '_collection_index_irn', 'int32'),
        ('ecatalogue.EntIndMaterial', 'Material', 'bool'),
        ('ecatalogue.EntIndType', 'Type', 'bool'),
        ('ecatalogue.EntIndMedia', 'Media', 'bool'),
        ('ecatalogue.EntIndBritish', 'British', 'bool'),
        ('ecatalogue.EntIndKindOfMaterial', 'Kind of material', 'string:100'),
        ('ecatalogue.EntIndKindOfMedia', 'Kind of media', 'string:100'),

        # Material detail
        ('ecatalogue.EntIndCount', 'Material count', 'string:100'),
        ('ecatalogue.EntIndSex', 'Material sex', 'string:100'),
        ('ecatalogue.EntIndStage', 'Material stage', 'string:100'),
        ('ecatalogue.EntIndTypes', 'Material types', 'string:100'),
        ('ecatalogue.EntIndPrimaryTypeNo', 'Material primary type no', 'string:100'),

        # Separate Botany and Entomology
        ('ecatalogue.ColDepartment', 'Department', 'string:100'),

        # Audit info
        ('ecatalogue.AdmDateModified', 'Modified', 'string:100'),
        ('ecatalogue.AdmDateInserted', 'Created', 'string:100'),
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

        # Convert booleans to yes / no for all columns in the main collection
        for (_, field, field_type) in self.get_collection_columns(self.collection_name):
            if field_type == 'bool':
                df[field][df[field] == 'True'] = 'Yes'
                df[field][df[field] == 'False'] = 'No'
                df[field][df[field] == 'N/A'] = ''

        # BUG FIX BS 140811
        # ColCurrentNameRef Is not being updated correctly - see record 899984
        # ColCurrentNameRef = 964105
        # Not a problem, as indexlots are using ColTaxonomicNameRef for summary data etc.,
        # So ColTaxonomicNameRef is the correct field to use.
        collection_index_columns = [
            ('_id', '_collection_index_irn', 'int32'),
            ('ColTaxonomicNameRef', '_taxonomy_irn', 'int32'),
            ('ColCurrentNameRef', '_current_name_irn', 'int32'),
        ]

        collection_index_irns = self._get_unique_irns(df, '_collection_index_irn')
        collection_index_df = self.get_dataframe(m, 'ecollectionindex', collection_index_columns, collection_index_irns, '_collection_index_irn')

        # Get all collection columns
        collection_columns = self.get_collection_columns()

        # And get the taxonomy for these collection
        taxonomy_irns = self._get_unique_irns(collection_index_df, '_taxonomy_irn')

        # The query to pre-load all taxonomy objects takes ~96 seconds
        # It is much faster to load taxonomy objects on the fly, for the current block
        # collection_index_irns = pd.unique(df._collection_index_irn.values.ravel()).tolist()
        taxonomy_df = self.get_dataframe(m, 'etaxonomy', collection_columns['etaxonomy'], taxonomy_irns, '_taxonomy_irn')

        # Merge the taxonomy into the collection index dataframe - we need to do this so we can merge into
        # main dataframe keyed by collection index ID
        collection_index_df = pd.merge(collection_index_df, taxonomy_df, how='inner', left_on=['_taxonomy_irn'], right_on=['_taxonomy_irn'])

        # Add current name - same process as the main taxonomy but using _current_name_irn source fields
        current_name_irns = self._get_unique_irns(collection_index_df, '_current_name_irn')
        current_name_df = self.get_dataframe(m, 'etaxonomy', collection_columns['etaxonomy2'], current_name_irns, '_current_name_irn')
        collection_index_df = pd.merge(collection_index_df, current_name_df, how='inner', left_on=['_current_name_irn'], right_on=['_current_name_irn'])

        # Merge results into main dataframe
        df = pd.merge(df, collection_index_df, how='outer', left_on=['_collection_index_irn'], right_on=['_collection_index_irn'])

        return df

    def get_output_columns(self):
        """
        Get a list of output columns, with bool converted to string:3 (so can be converted to Yes/No)
        @return:
        """
        return OrderedDict((col[1], 'string:3' if col[2] == 'bool' else col[2]) for col in self.columns if self._is_output_field(col[1]))


class IndexLotDatasetCSVTask(IndexLotDatasetTask, DatasetCSVTask):
    pass


class IndexLotDatasetAPITask(IndexLotDatasetTask, DatasetAPITask):
    pass

if __name__ == "__main__":
    luigi.run()