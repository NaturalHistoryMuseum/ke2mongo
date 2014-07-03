#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os

import codecs
import gzip


def error_handler(e):

    print e.encoding
    print e.start
    print e.object
    print 'ERROR'

    return (u'-',e.start + 1)



def main():

    f = '/vagrant/lib/default/src/ke2mongo/ke2mongo/tests/data/ecatalogue.export.20140522.gz'

    codecs.register_error('custom', error_handler)

    reader = codecs.getreader("latin-1")
    file_obj = reader(gzip.open(f, 'rb'), errors='custom')

    for line in file_obj:
        if 'Archeological Expedition' in line:
            print repr(line)
        # print repr(line)
        pass



if __name__ == '__main__':
    main()

