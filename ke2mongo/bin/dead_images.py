#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Quick script to identify when IT turned off images

"""

import pandas as pd
import requests
import time
from collections import OrderedDict
from ke2mongo.lib.mongo import mongo_client_db
from ke2mongo.tasks import MULTIMEDIA_FORMATS


def get_max_dimension(str_dimension):
    """
    Split the dimension string, and return the second highest value
    """
    dimensions = [int(x.strip()) for x in str_dimension.split(';')]
    return sorted(dimensions,reverse=True)[1]


def main():

        # Setup MongoDB
    mongo_db = mongo_client_db()

    q = {
        'MulMimeFormat': {'$in': MULTIMEDIA_FORMATS},
        'DocHeight': {'$exists': True},
        'DocWidth': {'$exists': True},
        'AdmPublishWebNoPasswordFlag': 'Y',
        'MulMimeType': 'image'
    }

    status = OrderedDict()

    total_failed = 0

    for d in pd.date_range(start='3/1/2014', end=pd.datetime.today()):

        date_str = str(d.date())

        q['AdmDateInserted'] = date_str

        print 'Checking date %s' % q['AdmDateInserted']

        status[date_str] = 0

        results = mongo_db.emultimedia.find(q).limit(1)

        if results.count():

            for record in results:
                url = 'http://www.nhm.ac.uk/emu-classes/class.EMuMedia.php?irn={_id}&image=yes&width={width}&height={height}'.format(
                    _id=record['_id'],
                    width=get_max_dimension(record['DocWidth']),
                    height=get_max_dimension(record['DocHeight'])
                )

                response = requests.head(url)

                # We only request jpeg images - but the error image is returned in png
                # So if image type == png, image request has failed
                failed = response.headers['content-type'] == 'image/png'

                if failed:
                    print 'Failed: %s' % date_str
                    # Count failures
                    status[date_str] += 1
                    total_failed += results.count()
                    print 'Total failed: %s' % total_failed

                # Pause so we don't kill the server
                time.sleep(0.5)

        else:

            print 'No images for %s' % date_str
            status[date_str] = None


    for d, failures in status.iteritems():
        print '%s: %s' % (d, failures)

    print '----------------'
    print 'Total failed: %s' % total_failed


if __name__ == '__main__':
    main()

