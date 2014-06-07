#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import numpy as np
import pandas as pd
from ke2mongo.tasks.specimen import SpecimenDatasetTask
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tasks import INDEX_LOT_TYPE

# TODO: This should be dep on taxonomy task running first

class IndexLotCSVTask(CSVTask):

    columns = [
        ('_id', '_id', 'int32'),
        ('EntIndIndexLotTaxonNameLocalRef', '_taxonomy_irn', 'int32'),
        ('EntIndMaterial', 'material', 'bool'),
        ('EntIndType', 'is_type', 'bool'),
        ('EntIndMedia', 'media', 'bool'),
        ('EntIndKindOfMaterial', 'kind_of_material', 'string:100'),
        ('EntIndKindOfMedia', 'kind_of_media', 'string:100'),
        # Material detail
        ('EntIndCount', 'material_count', 'string:100'),
        ('EntIndTypes', 'material_types', 'string:100'),
    ]

    query = {"ColRecordType": INDEX_LOT_TYPE}

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
        ('ClaRank', 'rank', 'string:10'),
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

        # The query to pre-load all taxonomy objects takes ~96 seconds
        # It is much faster to load taxonomy objects on the fly, for the current block
        irns = pd.unique(df._taxonomy_irn.values.ravel()).tolist()

        # Get the taxonomy data frame
        taxonomy_df = self.get_taxonomy(m, irns)

        # And merge into the catalogue records
        df = pd.merge(df, taxonomy_df, how='outer', left_on=['_taxonomy_irn'], right_on=['_taxonomy_irn'])
        return df

    def get_taxonomy(self, m, irns=None):
        """
        Get a data frame of taxonomy records
        @param m: Monary instance
        @param irns: taxonomy IRNs to retrieve
        @return:
        """
        ke_cols, df_cols, types = zip(*self.taxonomy_columns)

        q = {'_id': {'$in': irns}} if irns else {}

        query = m.query('keemu', 'etaxonomy', q, ke_cols, types)
        df = pd.DataFrame(np.matrix(query).transpose(), columns=df_cols)

        # Convert to int (adding index doesn't speed this up)
        df['_taxonomy_irn'] = df['_taxonomy_irn'].astype('int32')

        return df


class IndexLotDatasetTask(SpecimenDatasetTask):
    """
    Class for exporting exporting IndexLots data to CSV
    """
    name = 'Indexlots'
    description = 'Entomology Indexlot records'
    format = 'csv'

    csv_class = IndexLotCSVTask

