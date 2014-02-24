#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import getopt
import os
import gzip
from keparser import KEParser
from pymongo import MongoClient


# Location of the keemu schema
SCHEMA_FILE = '/vagrant/exports/schema.pl'
# The directory where the keemu export files are deposited
EXPORT_DIR = '/vagrant/exports'

# Types we don't want to include (copied from KE2SQL)
excluded_types = [
    'Acquisition',
    'Collection Level Description',
    'DNA Card', # 1 record, but keep an eye on this
    'Image',
    'Image (electronic)',
    'Image (non-digital)',
    'Image (digital)',
    'Incoming Loan',
    'L&A Catalogue',
    'Missing',
    'Object Entry',
    'object entry',  # FFS
    'Object entry',  # FFFS
    'PEG Specimen',
    'PEG Catalogue',
    'Preparation',
    'Rack File',
    'Tissue',  # Only 2 records. Watch.
    'Transient Lot'
]

class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg

def main(argv=None):

    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "f:l:", ["file=", "limit="])
        except getopt.error, msg:
            raise Usage(msg)

        limit = None
        file_name = None

        # option processing
        for option, value in opts:

            if option in ("-f", "--file"):
                file_name = value

            if option in ("-l", "--limit"):
                limit = value

        #  Us the first part of the file name as the mongo collection (ecatalogue)
        collection = file_name.split('.')[0]

        # Setup MongoDB
        client = MongoClient('127.0.0.1', 27017)
        mongo_db = client['keemu']

        try:

            file_path = os.path.join(EXPORT_DIR, file_name)

            if '.gz' in file_path:
                f = gzip.open(file_path, mode='rb')
            else:
                f = open(file_path, 'r')

            ke_data = KEParser(f, schema_file=SCHEMA_FILE, input_file_path=file_path)

            count = 0
            for record in ke_data:

                # Use the IRN as _id & remove original
                record['_id'] = record['irn']
                del record['irn']

                record_type = record.get('ColRecordType', 'Missing')

                # Only insert if it's a record type we want
                if record_type not in excluded_types:
                    mongo_db[collection].insert(record)

                status = ke_data.get_status()

                if status:
                    print status

                if limit and limit > count:
                    return

                count += 1

        except UnboundLocalError:
            raise Usage('Please specify an input file')

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        return 2

if __name__ == "__main__":
    sys.exit(main())

