#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
from ConfigParser import ConfigParser

config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.cfg'))