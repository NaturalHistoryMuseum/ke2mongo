#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.lib.mongo import mongo_client_db

CITES_COLLECTION = 'cites'

def get_cites_species():
    """
    Load cites species names from mongo

    These will already have been downloaded from http://checklist.cites.org/#/en in JSON
    And then loaded into the database with:

    mongoimport --db keemu --collection cites --type json --file /vagrant/exports/Index_of_CITES_Species_2014-10-17\ 17-34.json --jsonArray

    This should only be run if mongo is rebuilt - new records are marked as CITES on import

    @return: list
    """
    mongo_db = mongo_client_db()
    cursor = mongo_db[CITES_COLLECTION].find({'full_name': {'$ne': None}}, {'full_name':1})
    return [r['full_name'].encode('utf8') for r in cursor]