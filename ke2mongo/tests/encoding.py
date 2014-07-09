#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import unittest
from nose.tools import assert_equal
from nose.tools import assert_not_equal
from nose.tools import assert_raises
from nose.tools import raises
from ke2mongo import config
from ke2mongo.lib.ckan import call_action
import psycopg2

class TestEncodings(unittest.TestCase):

    @classmethod
    def setup_class(cls):

        conn_string = "host='localhost' dbname='datastore_default' user='ckan_default' password='asdf'"
        # print the connection string we will use to connect
        print "Connecting to database\n	->%s" % (conn_string)

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        conn.set_client_encoding('latin1')
        cls.cursor = conn.cursor()

    def test_1(self):

        data_dict = {
            'record_id': 1,
            'resource_id': 'cc6cd98e-1113-4186-91d7-054f8795dc48'
        }

        record = call_action('record_get', data_dict)


        x = record['SummaryData'].encode('raw_unicode_escape').decode('utf-8')
        print repr(x)
        print x
        assert 1 == 1

    def test_2(self):

        self.cursor.execute('SELECT "SummaryData" FROM "cc6cd98e-1113-4186-91d7-054f8795dc48"')
        records = self.cursor.fetchall()

        print records[0][0]
        print records[0][0].decode('utf-8')

        print self.cursor
        assert 1 == 1