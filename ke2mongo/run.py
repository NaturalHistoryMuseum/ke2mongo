#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MongoTask --local-scheduler

"""

import luigi
import ke2mongo.tasks

if __name__ == "__main__":
    luigi.run()