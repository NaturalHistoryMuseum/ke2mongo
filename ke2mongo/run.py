#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py MainTask --local-scheduler --date 20140522

python run.py SpecimenDatasetTask --local-scheduler --date 20140703
python run.py MongoMultimediaTask --local-scheduler --date 20140522

"""

import luigi
from ke2mongo.tasks.mongo_catalogue import MongoCatalogueTask
from ke2mongo.tasks.mongo_taxonomy import MongoTaxonomyTask
from ke2mongo.tasks.mongo_delete import MongoDeleteTask
from ke2mongo.tasks.mongo_multimedia import MongoMultimediaTask
from ke2mongo.tasks.specimen import SpecimenDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.main import MainTask

if __name__ == "__main__":
    luigi.run()