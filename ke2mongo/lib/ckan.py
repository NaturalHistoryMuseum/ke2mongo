#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import urllib2
import urllib
import json
from ke2mongo import config

def call_action(action, data_dict):
    """
    API Call
    @param action: API action
    @param data: dict
    @return:
    """
    url = '{site_url}/api/3/action/{action}'.format(
        site_url=config.get('ckan', 'site_url'),
        action=action
    )

    # Use the json module to dump a dictionary to a string for posting.
    data_string = urllib.quote(json.dumps(data_dict))

    # New urllib request
    request = urllib2.Request(url)

    request.add_header('Authorization', config.get('ckan', 'api_key'))

    # Make the HTTP request.
    response = urllib2.urlopen(request, data_string)
    # Ensure we have correct response code 200
    assert response.code == 200

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read())

    # Check the contents of the response.
    assert response_dict['success'] is True
    result = response_dict['result']

    return result
