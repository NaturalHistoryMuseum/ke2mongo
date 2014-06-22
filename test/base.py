#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os


# class BaseTask(object):
#
#     export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
#     file_name = luigi.Parameter()
#
#     @abc.abstractproperty
#     def module(self):
#         return None
#
#     def requires(self):
#         return [KEFileTask(module=self.module, export_dir=self.export_dir, compressed=False, file_name=self.file_name)]
#
#     def finish(self):
#         # Do not mark complete
#         pass