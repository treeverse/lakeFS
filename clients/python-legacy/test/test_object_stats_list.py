"""
    lakeFS API

    lakeFS HTTP API  # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: services@treeverse.io
    Generated by: https://openapi-generator.tech
"""


import sys
import unittest

import lakefs_client
from lakefs_client.model.object_stats import ObjectStats
from lakefs_client.model.pagination import Pagination
globals()['ObjectStats'] = ObjectStats
globals()['Pagination'] = Pagination
from lakefs_client.model.object_stats_list import ObjectStatsList


class TestObjectStatsList(unittest.TestCase):
    """ObjectStatsList unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testObjectStatsList(self):
        """Test ObjectStatsList"""
        # FIXME: construct object with mandatory attributes with example values
        # model = ObjectStatsList()  # noqa: E501
        pass


if __name__ == '__main__':
    unittest.main()
