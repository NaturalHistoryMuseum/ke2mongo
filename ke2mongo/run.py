#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MongoTask --local-scheduler

"""

import luigi
from ke2mongo.tasks.catalogue_mongo import CatalogueMongoTask
from ke2mongo.tasks.taxonomy_mongo import TaxonomyMongoTask
from ke2mongo.tasks.dwc import DarwinCoreDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask

if __name__ == "__main__":
    luigi.run()