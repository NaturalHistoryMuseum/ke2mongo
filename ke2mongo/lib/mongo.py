#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from pymongo import MongoClient
from ke2mongo import config


def mongo_client_db(database=config.get('mongo', 'database'), host=config.get('mongo', 'host')):
    return MongoClient(host)[database]
