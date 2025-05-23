# coding: utf-8

"""
    lakeFS API

    lakeFS HTTP API

    The version of the OpenAPI document: 1.0.0
    Contact: services@treeverse.io
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest
import datetime

from lakefs_sdk.models.import_status import ImportStatus  # noqa: E501

class TestImportStatus(unittest.TestCase):
    """ImportStatus unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> ImportStatus:
        """Test ImportStatus
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `ImportStatus`
        """
        model = ImportStatus()  # noqa: E501
        if include_optional:
            return ImportStatus(
                completed = True,
                update_time = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
                ingested_objects = 56,
                metarange_id = '',
                commit = lakefs_sdk.models.commit.Commit(
                    id = '', 
                    parents = [
                        ''
                        ], 
                    committer = '', 
                    message = '', 
                    creation_date = 56, 
                    meta_range_id = '', 
                    metadata = {
                        'key' : ''
                        }, 
                    generation = 56, 
                    version = 0, ),
                error = lakefs_sdk.models.error.Error(
                    message = '', )
            )
        else:
            return ImportStatus(
                completed = True,
                update_time = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
        )
        """

    def testImportStatus(self):
        """Test ImportStatus"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
