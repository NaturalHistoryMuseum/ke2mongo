#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi

class AllTask(luigi.Task):
    date = luigi.DateParameter(default=None)

    print date

    # lookback = luigi.IntParameter(default=14)
    # def requires(self):
    #     for i in xrange(self.lookback):
    #        date = self.date - datetime.timedelta(i + 1)
    #        yield SomeReport(date), SomeOtherReport(date), CropReport(date), TPSReport(date), FooBarBazReport(date)
