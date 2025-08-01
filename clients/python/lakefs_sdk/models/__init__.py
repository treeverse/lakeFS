# coding: utf-8

# flake8: noqa
"""
    lakeFS API

    lakeFS HTTP API

    The version of the OpenAPI document: 1.0.0
    Contact: services@treeverse.io
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


# import models into model package
from lakefs_sdk.models.acl import ACL
from lakefs_sdk.models.abort_presign_multipart_upload import AbortPresignMultipartUpload
from lakefs_sdk.models.access_key_credentials import AccessKeyCredentials
from lakefs_sdk.models.action_run import ActionRun
from lakefs_sdk.models.action_run_list import ActionRunList
from lakefs_sdk.models.auth_capabilities import AuthCapabilities
from lakefs_sdk.models.authentication_token import AuthenticationToken
from lakefs_sdk.models.branch_creation import BranchCreation
from lakefs_sdk.models.branch_protection_rule import BranchProtectionRule
from lakefs_sdk.models.cherry_pick_creation import CherryPickCreation
from lakefs_sdk.models.comm_prefs_input import CommPrefsInput
from lakefs_sdk.models.commit import Commit
from lakefs_sdk.models.commit_creation import CommitCreation
from lakefs_sdk.models.commit_list import CommitList
from lakefs_sdk.models.commit_overrides import CommitOverrides
from lakefs_sdk.models.commit_record_creation import CommitRecordCreation
from lakefs_sdk.models.complete_presign_multipart_upload import CompletePresignMultipartUpload
from lakefs_sdk.models.config import Config
from lakefs_sdk.models.copy_part_source import CopyPartSource
from lakefs_sdk.models.credentials import Credentials
from lakefs_sdk.models.credentials_list import CredentialsList
from lakefs_sdk.models.credentials_with_secret import CredentialsWithSecret
from lakefs_sdk.models.current_user import CurrentUser
from lakefs_sdk.models.custom_viewer import CustomViewer
from lakefs_sdk.models.diff import Diff
from lakefs_sdk.models.diff_list import DiffList
from lakefs_sdk.models.diff_object_stat import DiffObjectStat
from lakefs_sdk.models.error import Error
from lakefs_sdk.models.error_no_acl import ErrorNoACL
from lakefs_sdk.models.external_login_information import ExternalLoginInformation
from lakefs_sdk.models.external_principal import ExternalPrincipal
from lakefs_sdk.models.external_principal_creation import ExternalPrincipalCreation
from lakefs_sdk.models.external_principal_list import ExternalPrincipalList
from lakefs_sdk.models.find_merge_base_result import FindMergeBaseResult
from lakefs_sdk.models.garbage_collection_config import GarbageCollectionConfig
from lakefs_sdk.models.garbage_collection_prepare_response import GarbageCollectionPrepareResponse
from lakefs_sdk.models.garbage_collection_rule import GarbageCollectionRule
from lakefs_sdk.models.garbage_collection_rules import GarbageCollectionRules
from lakefs_sdk.models.group import Group
from lakefs_sdk.models.group_creation import GroupCreation
from lakefs_sdk.models.group_list import GroupList
from lakefs_sdk.models.hook_run import HookRun
from lakefs_sdk.models.hook_run_list import HookRunList
from lakefs_sdk.models.import_creation import ImportCreation
from lakefs_sdk.models.import_creation_response import ImportCreationResponse
from lakefs_sdk.models.import_location import ImportLocation
from lakefs_sdk.models.import_status import ImportStatus
from lakefs_sdk.models.installation_usage_report import InstallationUsageReport
from lakefs_sdk.models.internal_delete_branch_protection_rule_request import InternalDeleteBranchProtectionRuleRequest
from lakefs_sdk.models.license import License
from lakefs_sdk.models.login_config import LoginConfig
from lakefs_sdk.models.login_information import LoginInformation
from lakefs_sdk.models.merge import Merge
from lakefs_sdk.models.merge_result import MergeResult
from lakefs_sdk.models.meta_range_creation import MetaRangeCreation
from lakefs_sdk.models.meta_range_creation_response import MetaRangeCreationResponse
from lakefs_sdk.models.object_copy_creation import ObjectCopyCreation
from lakefs_sdk.models.object_error import ObjectError
from lakefs_sdk.models.object_error_list import ObjectErrorList
from lakefs_sdk.models.object_stage_creation import ObjectStageCreation
from lakefs_sdk.models.object_stats import ObjectStats
from lakefs_sdk.models.object_stats_list import ObjectStatsList
from lakefs_sdk.models.pagination import Pagination
from lakefs_sdk.models.path_list import PathList
from lakefs_sdk.models.policy import Policy
from lakefs_sdk.models.policy_list import PolicyList
from lakefs_sdk.models.prepare_gc_uncommitted_request import PrepareGCUncommittedRequest
from lakefs_sdk.models.prepare_gc_uncommitted_response import PrepareGCUncommittedResponse
from lakefs_sdk.models.presign_multipart_upload import PresignMultipartUpload
from lakefs_sdk.models.pull_request import PullRequest
from lakefs_sdk.models.pull_request_basic import PullRequestBasic
from lakefs_sdk.models.pull_request_creation import PullRequestCreation
from lakefs_sdk.models.pull_request_creation_response import PullRequestCreationResponse
from lakefs_sdk.models.pull_requests_list import PullRequestsList
from lakefs_sdk.models.range_metadata import RangeMetadata
from lakefs_sdk.models.ref import Ref
from lakefs_sdk.models.ref_list import RefList
from lakefs_sdk.models.refs_dump import RefsDump
from lakefs_sdk.models.refs_restore import RefsRestore
from lakefs_sdk.models.repository import Repository
from lakefs_sdk.models.repository_creation import RepositoryCreation
from lakefs_sdk.models.repository_dump_status import RepositoryDumpStatus
from lakefs_sdk.models.repository_list import RepositoryList
from lakefs_sdk.models.repository_metadata_keys import RepositoryMetadataKeys
from lakefs_sdk.models.repository_metadata_set import RepositoryMetadataSet
from lakefs_sdk.models.repository_restore_status import RepositoryRestoreStatus
from lakefs_sdk.models.reset_creation import ResetCreation
from lakefs_sdk.models.revert_creation import RevertCreation
from lakefs_sdk.models.setup import Setup
from lakefs_sdk.models.setup_state import SetupState
from lakefs_sdk.models.staging_location import StagingLocation
from lakefs_sdk.models.staging_metadata import StagingMetadata
from lakefs_sdk.models.statement import Statement
from lakefs_sdk.models.stats_event import StatsEvent
from lakefs_sdk.models.stats_events_list import StatsEventsList
from lakefs_sdk.models.storage_config import StorageConfig
from lakefs_sdk.models.storage_uri import StorageURI
from lakefs_sdk.models.sts_auth_request import StsAuthRequest
from lakefs_sdk.models.tag_creation import TagCreation
from lakefs_sdk.models.task_info import TaskInfo
from lakefs_sdk.models.ui_config import UIConfig
from lakefs_sdk.models.underlying_object_properties import UnderlyingObjectProperties
from lakefs_sdk.models.update_object_user_metadata import UpdateObjectUserMetadata
from lakefs_sdk.models.update_token import UpdateToken
from lakefs_sdk.models.upload_part import UploadPart
from lakefs_sdk.models.upload_part_copy_from import UploadPartCopyFrom
from lakefs_sdk.models.upload_part_from import UploadPartFrom
from lakefs_sdk.models.upload_to import UploadTo
from lakefs_sdk.models.usage_report import UsageReport
from lakefs_sdk.models.user import User
from lakefs_sdk.models.user_creation import UserCreation
from lakefs_sdk.models.user_list import UserList
from lakefs_sdk.models.version_config import VersionConfig
