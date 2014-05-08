#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
# import pymongo
# from pymongo import Connection

import gzip
import csv

from monary import Monary
import numpy as np
import pandas as pd
import time
import os

def get_taxonomy(m, irns=None):

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
    df['taxonomy_irn'] = df['taxonomy_irn'].astype('int32')

    return df


def is_type(arr, typeclass):
     return issubclass(arr.dtype.type, typeclass)

def main():



    # count = 0
    # with Monary("127.0.0.1") as m:
    #     for arrays in m.block_query('keemu', 'ecatalogue',{}, columns, types, block_size=8192):
    #         print arrays
    #         count += len(arrays[0])
    #
    # print "queried %i items" % count

    count = 0
    with Monary() as m:

        t1 = time.time()
        filename = '/tmp/blocks.csv'

        # Delete the file if it exists
        try:
            os.remove(filename)
        except OSError:
            pass

        # List of column tuples, in the format
        # (ke field name, new field name, data type, include in output True|False)
        columns = [
            ('_id', '_id', 'int32', True),
            ('EntIndIndexLotTaxonNameLocalRef', 'taxonomy_irn', 'int32', False),
            ('EntIndMaterial', 'material', 'bool', True),
            ('EntIndType', 'is_type', 'bool', True),
            ('EntIndMedia', 'media', 'bool', True),
            ('EntIndKindOfMaterial', 'kind_of_material', 'string:100', True),
            ('EntIndKindOfMedia', 'kind_of_media', 'string:100', True),
            # Material detail
            ('EntIndCount', 'kind_of_media', 'string:100', True),
            ('EntIndTypes', 'kind_of_media', 'string:100', True),
        ]

        ke_cols, df_cols, types, output = zip(*columns)

        t1 = time.time()

        catalogue_blocks = m.block_query('keemu', 'ecatalogue', {"ColRecordType": "Index Lot", "_id": {'$in': [1148875]}}, ke_cols, types)
        # catalogue_blocks = m.block_query('keemu', 'ecatalogue', {"ColRecordType": "Index Lot"}, ke_cols, types)

        for catalogue_block in catalogue_blocks:

            # If this a field to be output (otherwise it'll be a join, map to string)
            catalogue_block = [arr.astype(np.str).filled('') if output[i] else arr for i, arr in enumerate(catalogue_block)]

            df = pd.DataFrame(np.matrix(catalogue_block).transpose(), columns=df_cols)

            # Ensure IRN is an int
            df['taxonomy_irn'] = df['taxonomy_irn'].astype('int32')

            # # The query to pre-load all taxonomy objects takes ~96 seconds
            # # It is much faster to load taxonomy objects on the fly, for the current block
            irns = pd.unique(df.taxonomy_irn.values.ravel()).tolist()

            taxonomy_df = get_taxonomy(m, irns)

            df = pd.merge(df, taxonomy_df, how='outer', left_on=['taxonomy_irn'], right_on=['taxonomy_irn'])

            # TODO: Output Cols
            # TODO: SQL field type
            output_columns = list(df.columns.values)
            # output_columns.remove('taxonomy_irn')

            df.to_csv(filename, chunksize=1000, mode='a', cols=output_columns, index=False)

        t2 = time.time()
        print('BLock query time: %.2f secs' % (t2 - t1))


        # catalogue_df = pandas.DataFrame(numpy.matrix(block_query).transpose(), columns=columns)
        # catalogue_df.to_csv('/tmp/pd.csv', chunksize=1000, cols=['sci_name'])




        # print 'HEY'
        #

        #
        # print 'CAT Q'
        #
        # types = ['string:100', 'int32']
        #
        # catalogue_query = m.query('keemu', 'ecatalogue', {'ColRecordType': 'Index Lot'}, columns, types)
        #
        # print 'Creating data frame'
        # catalogue_df = pandas.DataFrame(numpy.matrix(catalogue_query).transpose(), columns=columns)
        #
        # # catalogue = catalog
        #
        # t1 = time.time()
        #
        # tidx = catalogue_df.EntIndIndexLotTaxonNameLocalRef.dropna()
        # catalogue_df=catalogue_df.ix[tidx.index]
        #
        # catalogue_df = pandas.merge(catalogue_df, taxonomy_df, how='outer', left_on=['EntIndIndexLotTaxonNameLocalRef'], right_on=['taxonomy_irn'])
        # t2 = time.time()
        # print( '  - Merge took %.2f secs' % (t2 - t1))
        # # df.join(taxonomy, on='EntIndIndexLotTaxonNameLocalRef')
        #
        # print catalogue_df
        #
        # print 'Writing CSV'
        # catalogue_df.to_csv('/tmp/pd.csv', chunksize=1000, cols=['sci_name'])


    print "queried %i items" % count



    print 'HEY'

    # print numpy_arrays

    # connection = Connection()
    # db = connection.keemu
    # docs = db.ecatalogue
    #
    # x = 0
    #
    # with gzip.open("/tmp/test.csv.gz", "w") as f:
    #
    #     csv_w=csv.writer(f)
    #
    #     for doc in docs.find():
    #         csv_w.writerow(list(doc))
    #
    #         x+=1
    #         print x


if __name__ == '__main__':
    main()

