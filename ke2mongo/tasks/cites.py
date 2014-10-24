#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

"""


import luigi
from ke2mongo.log import log
from ke2mongo.lib.timeit import timeit
from ke2mongo.lib.cites import get_cites_species
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.lib.mongo import mongo_client_db


class CitesTask(luigi.Task):

    """
    Very basic task to update ecatalogue records with sites data
    """
    @timeit
    def run(self):

        mongo_db = mongo_client_db()
        collection = MongoCatalogueTask(date=None).collection_name
        cites_species = get_cites_species()

        # Set cites=true flag
        cites_records_cursor = mongo_db[collection].update({'DarScientificName': {'$in': cites_species}}, {'$set': {'cites': True}}, multi=True)
        log.info('Updated %s catalogue records as CITES', cites_records_cursor['nModified'])

if __name__ == '__main__':
    luigi.run()