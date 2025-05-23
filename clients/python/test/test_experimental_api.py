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

from lakefs_sdk.api.experimental_api import ExperimentalApi  # noqa: E501


class TestExperimentalApi(unittest.TestCase):
    """ExperimentalApi unit test stubs"""

    def setUp(self) -> None:
        self.api = ExperimentalApi()  # noqa: E501

    def tearDown(self) -> None:
        pass

    def test_abort_presign_multipart_upload(self) -> None:
        """Test case for abort_presign_multipart_upload

        Abort a presign multipart upload  # noqa: E501
        """
        pass

    def test_complete_presign_multipart_upload(self) -> None:
        """Test case for complete_presign_multipart_upload

        Complete a presign multipart upload request  # noqa: E501
        """
        pass

    def test_create_presign_multipart_upload(self) -> None:
        """Test case for create_presign_multipart_upload

        Initiate a multipart upload  # noqa: E501
        """
        pass

    def test_create_pull_request(self) -> None:
        """Test case for create_pull_request

        create pull request  # noqa: E501
        """
        pass

    def test_create_user_external_principal(self) -> None:
        """Test case for create_user_external_principal

        attach external principal to user  # noqa: E501
        """
        pass

    def test_delete_user_external_principal(self) -> None:
        """Test case for delete_user_external_principal

        delete external principal from user  # noqa: E501
        """
        pass

    def test_external_principal_login(self) -> None:
        """Test case for external_principal_login

        perform a login using an external authenticator  # noqa: E501
        """
        pass

    def test_get_external_principal(self) -> None:
        """Test case for get_external_principal

        describe external principal by id  # noqa: E501
        """
        pass

    def test_get_license(self) -> None:
        """Test case for get_license

        """
        pass

    def test_get_pull_request(self) -> None:
        """Test case for get_pull_request

        get pull request  # noqa: E501
        """
        pass

    def test_hard_reset_branch(self) -> None:
        """Test case for hard_reset_branch

        hard reset branch  # noqa: E501
        """
        pass

    def test_list_pull_requests(self) -> None:
        """Test case for list_pull_requests

        list pull requests  # noqa: E501
        """
        pass

    def test_list_user_external_principals(self) -> None:
        """Test case for list_user_external_principals

        list user external policies attached to a user  # noqa: E501
        """
        pass

    def test_merge_pull_request(self) -> None:
        """Test case for merge_pull_request

        merge pull request  # noqa: E501
        """
        pass

    def test_sts_login(self) -> None:
        """Test case for sts_login

        perform a login with STS  # noqa: E501
        """
        pass

    def test_update_object_user_metadata(self) -> None:
        """Test case for update_object_user_metadata

        rewrite (all) object metadata  # noqa: E501
        """
        pass

    def test_update_pull_request(self) -> None:
        """Test case for update_pull_request

        update pull request  # noqa: E501
        """
        pass

    def test_upload_part(self) -> None:
        """Test case for upload_part

        """
        pass

    def test_upload_part_copy(self) -> None:
        """Test case for upload_part_copy

        """
        pass


if __name__ == '__main__':
    unittest.main()
