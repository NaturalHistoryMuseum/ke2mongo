#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

If


"""

import luigi
from ke2mongo.tasks.specimen import SpecimenDatasetAPITask
from ke2mongo.tasks.indexlot import IndexLotDatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetAPITask

class SolrTask(luigi.Task):
    """
    Solr reindex task
    """

    date = luigi.IntParameter()

    # indexes = config.get('solr', 'indexes').split(',')

    def run(self):
        print('SOLR')