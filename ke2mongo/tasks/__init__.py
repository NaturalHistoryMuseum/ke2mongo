#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

PARENT_TYPES = [
    'Bird Group Parent',
    'Mammal Group Parent',
]

PART_TYPES = [
    'Bird Group Part',
    'Egg',
    'Nest',
    'Mammal Group Part'
]

MULTIMEDIA_URL = 'http://www.nhm.ac.uk/emu-classes/class.EMuMedia.php?irn=%s&image=yes&width=600'

# http://www.nhm.ac.uk/emu-classes/class.EMuMedia.php only supports jp2 and jpeg - not gif, tiff etc.,
MULTIMEDIA_FORMATS = ['jp2', 'jpeg']

DATE_FORMAT = 'YYYY-MM-DD'