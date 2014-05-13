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
from ke2mongo.tasks.csv import CSVTask

class IndexLotDatasetTask(CSVTask):
    """
    Class for exporting exporting IndexLots data to CSV
    """

    columns = [
        ('_id', '_id', 'int32', True),
        ('EntIndIndexLotTaxonNameLocalRef', '_taxonomy_irn', 'int32', False),
        ('EntIndMaterial', 'material', 'bool', True),
        ('EntIndType', 'is_type', 'bool', True),
        ('EntIndMedia', 'media', 'bool', True),
        ('EntIndKindOfMaterial', 'kind_of_material', 'string:100', True),
        ('EntIndKindOfMedia', 'kind_of_media', 'string:100', True),
        # Material detail
        ('EntIndCount', 'material_count', 'string:100', True),
        ('EntIndTypes', 'material_types', 'string:100', True),
    ]

    query = {"ColRecordType": "Index Lot"}

    outfile = '/tmp/indexlots.csv'

    def process(self, m, df):

        # The query to pre-load all taxonomy objects takes ~96 seconds
        # It is much faster to load taxonomy objects on the fly, for the current block
        irns = pd.unique(df.taxonomy_irn.values.ravel()).tolist()

        # Get the taxonomy data frame
        taxonomy_df = self.get_taxonomy(m, irns)

        # And merge into the catalogue records
        df = pd.merge(df, taxonomy_df, how='outer', left_on=['_taxonomy_irn'], right_on=['_taxonomy_irn'])
        return df

    @staticmethod
    def get_taxonomy(m, irns=None):
        """
        Get a data frame of taxonomy records
        @param m: Monary instance
        @param irns: taxonomy IRNs to retrieve
        @return:
        """
        columns = [
            ('_id', 'taxonomy_irn', 'int32'),
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

        ke_cols, df_cols, types = zip(*columns)

        q = {'_id': {'$in': irns}} if irns else {}

        query = m.query('keemu', 'etaxonomy', q, ke_cols, types)
        df = pd.DataFrame(np.matrix(query).transpose(), columns=df_cols)

        # Convert to int (adding index doesn't speed this up)
        df['_taxonomy_irn'] = df['_taxonomy_irn'].astype('int32')

        return df