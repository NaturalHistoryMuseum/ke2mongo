#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import unittest
import csv
import codecs

# class MongoTask(luigi.Task):
#
#     def requires(self):
#         return KEFileTask(module='encodings', date=0)



class EncodingsTest(unittest.TestCase):

    def test_1(self):

        f = '/tmp/csvtesttask.csv'

        # with open(f, 'rb') as csv_file:
        #     csv_file_reader = csv.reader(csv_file)
        #     for row in csv_file_reader:
        #         print row[0]
        #         print unicode(row[0], "utf-8")
                # print

        with codecs.open(f, 'r', encoding='utf-8') as csv_file_reader:
            for row in csv_file_reader:
                print row





if __name__ == '__main__':
    unittest.main()

