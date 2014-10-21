#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from pymongo import MongoClient
from ke2mongo import config

CITES_COLLECTION = 'cites'

def get_cites_species():
    """
    Load cites species names from mongo

    These will already have been downloaded from http://checklist.cites.org/#/en in JSON
    And then loaded into the database with:

    mongoimport --db keemu --collection cites --type json --file /vagrant/exports/Index_of_CITES_Species_2014-10-17\ 17-34.json --jsonArray

    @return: list
    """

    mongo_client = MongoClient()
    db = config.get('mongo', 'database')
    cursor = mongo_client[db][CITES_COLLECTION].find({'full_name': {'$ne': None}}, {'full_name':1})
    return [r['full_name'] for r in cursor]