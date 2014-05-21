#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.


Decorator for timing function

"""

import time

def timeit(method):
    """
    Python decorator for measuring the execution time of functions / methods

    Usage:

    @timeit
    def func():

    """
    def timed(*args, **kw):

        t1 = time.time()
        result = method(*args, **kw)
        t2 = time.time()
        print 'Time: %r %2.2f sec' % (args, t2-t1)
        return result

    return timed
