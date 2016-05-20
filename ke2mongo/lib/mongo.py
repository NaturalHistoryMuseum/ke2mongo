#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import re
import luigi
from collections import OrderedDict
from pymongo import MongoClient
from ke2mongo import config


def mongo_client_db(database=config.get('mongo', 'database'), host=config.get('mongo', 'host')):
    return MongoClient(host)[database]


def mongo_get_marker_collection_name():
    return luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')


def mongo_get_update_markers():

    mongo_db = mongo_client_db()
    marker_collection_name = mongo_get_marker_collection_name()
    cursor = mongo_db[marker_collection_name].find()

    re_update_id = re.compile('([a-zA-Z]+)\(date=([0-9]+)\)')

    # OrderedDict to store all of the update classes
    update_markers = OrderedDict()

    for record in cursor:
        result = re_update_id.match(record['update_id'])
        if result:
            update_cls = result.group(1)
            update_date = int(result.group(2))
            try:
                update_markers[update_date].append(update_cls)
            except KeyError:
                update_markers[update_date] = [update_cls]

    return update_markers