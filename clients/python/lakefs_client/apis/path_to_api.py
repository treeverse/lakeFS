import typing_extensions

from lakefs_client.paths import PathValues
from lakefs_client.apis.paths.setup_comm_prefs import SetupCommPrefs
from lakefs_client.apis.paths.setup_lakefs import SetupLakefs
from lakefs_client.apis.paths.user import User
from lakefs_client.apis.paths.auth_login import AuthLogin
from lakefs_client.apis.paths.auth_password import AuthPassword
from lakefs_client.apis.paths.auth_password_forgot import AuthPasswordForgot
from lakefs_client.apis.paths.auth_capabilities import AuthCapabilities
from lakefs_client.apis.paths.auth_users import AuthUsers
from lakefs_client.apis.paths.auth_users_user_id import AuthUsersUserId
from lakefs_client.apis.paths.auth_groups import AuthGroups
from lakefs_client.apis.paths.auth_groups_group_id import AuthGroupsGroupId
from lakefs_client.apis.paths.auth_policies import AuthPolicies
from lakefs_client.apis.paths.auth_policies_policy_id import AuthPoliciesPolicyId
from lakefs_client.apis.paths.auth_groups_group_id_members import AuthGroupsGroupIdMembers
from lakefs_client.apis.paths.auth_groups_group_id_members_user_id import AuthGroupsGroupIdMembersUserId
from lakefs_client.apis.paths.auth_users_user_id_credentials import AuthUsersUserIdCredentials
from lakefs_client.apis.paths.auth_users_user_id_credentials_access_key_id import AuthUsersUserIdCredentialsAccessKeyId
from lakefs_client.apis.paths.auth_users_user_id_groups import AuthUsersUserIdGroups
from lakefs_client.apis.paths.auth_users_user_id_policies import AuthUsersUserIdPolicies
from lakefs_client.apis.paths.auth_users_user_id_policies_policy_id import AuthUsersUserIdPoliciesPolicyId
from lakefs_client.apis.paths.auth_groups_group_id_policies import AuthGroupsGroupIdPolicies
from lakefs_client.apis.paths.auth_groups_group_id_policies_policy_id import AuthGroupsGroupIdPoliciesPolicyId
from lakefs_client.apis.paths.auth_groups_group_id_acl import AuthGroupsGroupIdAcl
from lakefs_client.apis.paths.repositories import Repositories
from lakefs_client.apis.paths.repositories_repository import RepositoriesRepository
from lakefs_client.apis.paths.repositories_repository_otf_refs_left_ref_diff_right_ref import RepositoriesRepositoryOtfRefsLeftRefDiffRightRef
from lakefs_client.apis.paths.repositories_repository_refs_dump import RepositoriesRepositoryRefsDump
from lakefs_client.apis.paths.repositories_repository_refs_restore import RepositoriesRepositoryRefsRestore
from lakefs_client.apis.paths.repositories_repository_tags import RepositoriesRepositoryTags
from lakefs_client.apis.paths.repositories_repository_tags_tag import RepositoriesRepositoryTagsTag
from lakefs_client.apis.paths.repositories_repository_branches import RepositoriesRepositoryBranches
from lakefs_client.apis.paths.repositories_repository_refs_ref_commits import RepositoriesRepositoryRefsRefCommits
from lakefs_client.apis.paths.repositories_repository_branches_branch_commits import RepositoriesRepositoryBranchesBranchCommits
from lakefs_client.apis.paths.repositories_repository_branches_branch import RepositoriesRepositoryBranchesBranch
from lakefs_client.apis.paths.repositories_repository_branches_branch_revert import RepositoriesRepositoryBranchesBranchRevert
from lakefs_client.apis.paths.repositories_repository_branches_branch_cherry_pick import RepositoriesRepositoryBranchesBranchCherryPick
from lakefs_client.apis.paths.repositories_repository_refs_source_ref_merge_destination_branch import RepositoriesRepositoryRefsSourceRefMergeDestinationBranch
from lakefs_client.apis.paths.repositories_repository_branches_branch_diff import RepositoriesRepositoryBranchesBranchDiff
from lakefs_client.apis.paths.repositories_repository_refs_left_ref_diff_right_ref import RepositoriesRepositoryRefsLeftRefDiffRightRef
from lakefs_client.apis.paths.repositories_repository_commits_commit_id import RepositoriesRepositoryCommitsCommitId
from lakefs_client.apis.paths.repositories_repository_refs_ref_objects import RepositoriesRepositoryRefsRefObjects
from lakefs_client.apis.paths.repositories_repository_branches_branch_staging_backing import RepositoriesRepositoryBranchesBranchStagingBacking
from lakefs_client.apis.paths.repositories_repository_branches_metaranges import RepositoriesRepositoryBranchesMetaranges
from lakefs_client.apis.paths.repositories_repository_branches_ranges import RepositoriesRepositoryBranchesRanges
from lakefs_client.apis.paths.repositories_repository_branches_branch_objects import RepositoriesRepositoryBranchesBranchObjects
from lakefs_client.apis.paths.repositories_repository_branches_branch_objects_delete import RepositoriesRepositoryBranchesBranchObjectsDelete
from lakefs_client.apis.paths.repositories_repository_branches_branch_objects_copy import RepositoriesRepositoryBranchesBranchObjectsCopy
from lakefs_client.apis.paths.repositories_repository_refs_ref_objects_stat import RepositoriesRepositoryRefsRefObjectsStat
from lakefs_client.apis.paths.repositories_repository_refs_ref_objects_underlying_properties import RepositoriesRepositoryRefsRefObjectsUnderlyingProperties
from lakefs_client.apis.paths.repositories_repository_refs_ref_objects_ls import RepositoriesRepositoryRefsRefObjectsLs
from lakefs_client.apis.paths.repositories_repository_refs_branch_symlink import RepositoriesRepositoryRefsBranchSymlink
from lakefs_client.apis.paths.repositories_repository_actions_runs import RepositoriesRepositoryActionsRuns
from lakefs_client.apis.paths.repositories_repository_actions_runs_run_id import RepositoriesRepositoryActionsRunsRunId
from lakefs_client.apis.paths.repositories_repository_actions_runs_run_id_hooks import RepositoriesRepositoryActionsRunsRunIdHooks
from lakefs_client.apis.paths.repositories_repository_actions_runs_run_id_hooks_hook_run_id_output import RepositoriesRepositoryActionsRunsRunIdHooksHookRunIdOutput
from lakefs_client.apis.paths.repositories_repository_metadata_meta_range_meta_range import RepositoriesRepositoryMetadataMetaRangeMetaRange
from lakefs_client.apis.paths.repositories_repository_metadata_range_range import RepositoriesRepositoryMetadataRangeRange
from lakefs_client.apis.paths.repositories_repository_gc_rules import RepositoriesRepositoryGcRules
from lakefs_client.apis.paths.repositories_repository_gc_prepare_commits import RepositoriesRepositoryGcPrepareCommits
from lakefs_client.apis.paths.repositories_repository_gc_prepare_uncommited import RepositoriesRepositoryGcPrepareUncommited
from lakefs_client.apis.paths.repositories_repository_branch_protection import RepositoriesRepositoryBranchProtection
from lakefs_client.apis.paths.healthcheck import Healthcheck
from lakefs_client.apis.paths.config_version import ConfigVersion
from lakefs_client.apis.paths.config_storage import ConfigStorage
from lakefs_client.apis.paths.config_garbage_collection import ConfigGarbageCollection
from lakefs_client.apis.paths.templates_template_location import TemplatesTemplateLocation
from lakefs_client.apis.paths.statistics import Statistics

PathToApi = typing_extensions.TypedDict(
    'PathToApi',
    {
        PathValues.SETUP_COMM_PREFS: SetupCommPrefs,
        PathValues.SETUP_LAKEFS: SetupLakefs,
        PathValues.USER: User,
        PathValues.AUTH_LOGIN: AuthLogin,
        PathValues.AUTH_PASSWORD: AuthPassword,
        PathValues.AUTH_PASSWORD_FORGOT: AuthPasswordForgot,
        PathValues.AUTH_CAPABILITIES: AuthCapabilities,
        PathValues.AUTH_USERS: AuthUsers,
        PathValues.AUTH_USERS_USER_ID: AuthUsersUserId,
        PathValues.AUTH_GROUPS: AuthGroups,
        PathValues.AUTH_GROUPS_GROUP_ID: AuthGroupsGroupId,
        PathValues.AUTH_POLICIES: AuthPolicies,
        PathValues.AUTH_POLICIES_POLICY_ID: AuthPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_MEMBERS: AuthGroupsGroupIdMembers,
        PathValues.AUTH_GROUPS_GROUP_ID_MEMBERS_USER_ID: AuthGroupsGroupIdMembersUserId,
        PathValues.AUTH_USERS_USER_ID_CREDENTIALS: AuthUsersUserIdCredentials,
        PathValues.AUTH_USERS_USER_ID_CREDENTIALS_ACCESS_KEY_ID: AuthUsersUserIdCredentialsAccessKeyId,
        PathValues.AUTH_USERS_USER_ID_GROUPS: AuthUsersUserIdGroups,
        PathValues.AUTH_USERS_USER_ID_POLICIES: AuthUsersUserIdPolicies,
        PathValues.AUTH_USERS_USER_ID_POLICIES_POLICY_ID: AuthUsersUserIdPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_POLICIES: AuthGroupsGroupIdPolicies,
        PathValues.AUTH_GROUPS_GROUP_ID_POLICIES_POLICY_ID: AuthGroupsGroupIdPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_ACL: AuthGroupsGroupIdAcl,
        PathValues.REPOSITORIES: Repositories,
        PathValues.REPOSITORIES_REPOSITORY: RepositoriesRepository,
        PathValues.REPOSITORIES_REPOSITORY_OTF_REFS_LEFT_REF_DIFF_RIGHT_REF: RepositoriesRepositoryOtfRefsLeftRefDiffRightRef,
        PathValues.REPOSITORIES_REPOSITORY_REFS_DUMP: RepositoriesRepositoryRefsDump,
        PathValues.REPOSITORIES_REPOSITORY_REFS_RESTORE: RepositoriesRepositoryRefsRestore,
        PathValues.REPOSITORIES_REPOSITORY_TAGS: RepositoriesRepositoryTags,
        PathValues.REPOSITORIES_REPOSITORY_TAGS_TAG: RepositoriesRepositoryTagsTag,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES: RepositoriesRepositoryBranches,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_COMMITS: RepositoriesRepositoryRefsRefCommits,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_COMMITS: RepositoriesRepositoryBranchesBranchCommits,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH: RepositoriesRepositoryBranchesBranch,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_REVERT: RepositoriesRepositoryBranchesBranchRevert,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_CHERRYPICK: RepositoriesRepositoryBranchesBranchCherryPick,
        PathValues.REPOSITORIES_REPOSITORY_REFS_SOURCE_REF_MERGE_DESTINATION_BRANCH: RepositoriesRepositoryRefsSourceRefMergeDestinationBranch,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_DIFF: RepositoriesRepositoryBranchesBranchDiff,
        PathValues.REPOSITORIES_REPOSITORY_REFS_LEFT_REF_DIFF_RIGHT_REF: RepositoriesRepositoryRefsLeftRefDiffRightRef,
        PathValues.REPOSITORIES_REPOSITORY_COMMITS_COMMIT_ID: RepositoriesRepositoryCommitsCommitId,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS: RepositoriesRepositoryRefsRefObjects,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_STAGING_BACKING: RepositoriesRepositoryBranchesBranchStagingBacking,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_METARANGES: RepositoriesRepositoryBranchesMetaranges,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_RANGES: RepositoriesRepositoryBranchesRanges,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS: RepositoriesRepositoryBranchesBranchObjects,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS_DELETE: RepositoriesRepositoryBranchesBranchObjectsDelete,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS_COPY: RepositoriesRepositoryBranchesBranchObjectsCopy,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_STAT: RepositoriesRepositoryRefsRefObjectsStat,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_UNDERLYING_PROPERTIES: RepositoriesRepositoryRefsRefObjectsUnderlyingProperties,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_LS: RepositoriesRepositoryRefsRefObjectsLs,
        PathValues.REPOSITORIES_REPOSITORY_REFS_BRANCH_SYMLINK: RepositoriesRepositoryRefsBranchSymlink,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS: RepositoriesRepositoryActionsRuns,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID: RepositoriesRepositoryActionsRunsRunId,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID_HOOKS: RepositoriesRepositoryActionsRunsRunIdHooks,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID_HOOKS_HOOK_RUN_ID_OUTPUT: RepositoriesRepositoryActionsRunsRunIdHooksHookRunIdOutput,
        PathValues.REPOSITORIES_REPOSITORY_METADATA_META_RANGE_META_RANGE: RepositoriesRepositoryMetadataMetaRangeMetaRange,
        PathValues.REPOSITORIES_REPOSITORY_METADATA_RANGE_RANGE: RepositoriesRepositoryMetadataRangeRange,
        PathValues.REPOSITORIES_REPOSITORY_GC_RULES: RepositoriesRepositoryGcRules,
        PathValues.REPOSITORIES_REPOSITORY_GC_PREPARE_COMMITS: RepositoriesRepositoryGcPrepareCommits,
        PathValues.REPOSITORIES_REPOSITORY_GC_PREPARE_UNCOMMITED: RepositoriesRepositoryGcPrepareUncommited,
        PathValues.REPOSITORIES_REPOSITORY_BRANCH_PROTECTION: RepositoriesRepositoryBranchProtection,
        PathValues.HEALTHCHECK: Healthcheck,
        PathValues.CONFIG_VERSION: ConfigVersion,
        PathValues.CONFIG_STORAGE: ConfigStorage,
        PathValues.CONFIG_GARBAGECOLLECTION: ConfigGarbageCollection,
        PathValues.TEMPLATES_TEMPLATE_LOCATION: TemplatesTemplateLocation,
        PathValues.STATISTICS: Statistics,
    }
)

path_to_api = PathToApi(
    {
        PathValues.SETUP_COMM_PREFS: SetupCommPrefs,
        PathValues.SETUP_LAKEFS: SetupLakefs,
        PathValues.USER: User,
        PathValues.AUTH_LOGIN: AuthLogin,
        PathValues.AUTH_PASSWORD: AuthPassword,
        PathValues.AUTH_PASSWORD_FORGOT: AuthPasswordForgot,
        PathValues.AUTH_CAPABILITIES: AuthCapabilities,
        PathValues.AUTH_USERS: AuthUsers,
        PathValues.AUTH_USERS_USER_ID: AuthUsersUserId,
        PathValues.AUTH_GROUPS: AuthGroups,
        PathValues.AUTH_GROUPS_GROUP_ID: AuthGroupsGroupId,
        PathValues.AUTH_POLICIES: AuthPolicies,
        PathValues.AUTH_POLICIES_POLICY_ID: AuthPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_MEMBERS: AuthGroupsGroupIdMembers,
        PathValues.AUTH_GROUPS_GROUP_ID_MEMBERS_USER_ID: AuthGroupsGroupIdMembersUserId,
        PathValues.AUTH_USERS_USER_ID_CREDENTIALS: AuthUsersUserIdCredentials,
        PathValues.AUTH_USERS_USER_ID_CREDENTIALS_ACCESS_KEY_ID: AuthUsersUserIdCredentialsAccessKeyId,
        PathValues.AUTH_USERS_USER_ID_GROUPS: AuthUsersUserIdGroups,
        PathValues.AUTH_USERS_USER_ID_POLICIES: AuthUsersUserIdPolicies,
        PathValues.AUTH_USERS_USER_ID_POLICIES_POLICY_ID: AuthUsersUserIdPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_POLICIES: AuthGroupsGroupIdPolicies,
        PathValues.AUTH_GROUPS_GROUP_ID_POLICIES_POLICY_ID: AuthGroupsGroupIdPoliciesPolicyId,
        PathValues.AUTH_GROUPS_GROUP_ID_ACL: AuthGroupsGroupIdAcl,
        PathValues.REPOSITORIES: Repositories,
        PathValues.REPOSITORIES_REPOSITORY: RepositoriesRepository,
        PathValues.REPOSITORIES_REPOSITORY_OTF_REFS_LEFT_REF_DIFF_RIGHT_REF: RepositoriesRepositoryOtfRefsLeftRefDiffRightRef,
        PathValues.REPOSITORIES_REPOSITORY_REFS_DUMP: RepositoriesRepositoryRefsDump,
        PathValues.REPOSITORIES_REPOSITORY_REFS_RESTORE: RepositoriesRepositoryRefsRestore,
        PathValues.REPOSITORIES_REPOSITORY_TAGS: RepositoriesRepositoryTags,
        PathValues.REPOSITORIES_REPOSITORY_TAGS_TAG: RepositoriesRepositoryTagsTag,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES: RepositoriesRepositoryBranches,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_COMMITS: RepositoriesRepositoryRefsRefCommits,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_COMMITS: RepositoriesRepositoryBranchesBranchCommits,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH: RepositoriesRepositoryBranchesBranch,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_REVERT: RepositoriesRepositoryBranchesBranchRevert,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_CHERRYPICK: RepositoriesRepositoryBranchesBranchCherryPick,
        PathValues.REPOSITORIES_REPOSITORY_REFS_SOURCE_REF_MERGE_DESTINATION_BRANCH: RepositoriesRepositoryRefsSourceRefMergeDestinationBranch,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_DIFF: RepositoriesRepositoryBranchesBranchDiff,
        PathValues.REPOSITORIES_REPOSITORY_REFS_LEFT_REF_DIFF_RIGHT_REF: RepositoriesRepositoryRefsLeftRefDiffRightRef,
        PathValues.REPOSITORIES_REPOSITORY_COMMITS_COMMIT_ID: RepositoriesRepositoryCommitsCommitId,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS: RepositoriesRepositoryRefsRefObjects,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_STAGING_BACKING: RepositoriesRepositoryBranchesBranchStagingBacking,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_METARANGES: RepositoriesRepositoryBranchesMetaranges,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_RANGES: RepositoriesRepositoryBranchesRanges,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS: RepositoriesRepositoryBranchesBranchObjects,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS_DELETE: RepositoriesRepositoryBranchesBranchObjectsDelete,
        PathValues.REPOSITORIES_REPOSITORY_BRANCHES_BRANCH_OBJECTS_COPY: RepositoriesRepositoryBranchesBranchObjectsCopy,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_STAT: RepositoriesRepositoryRefsRefObjectsStat,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_UNDERLYING_PROPERTIES: RepositoriesRepositoryRefsRefObjectsUnderlyingProperties,
        PathValues.REPOSITORIES_REPOSITORY_REFS_REF_OBJECTS_LS: RepositoriesRepositoryRefsRefObjectsLs,
        PathValues.REPOSITORIES_REPOSITORY_REFS_BRANCH_SYMLINK: RepositoriesRepositoryRefsBranchSymlink,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS: RepositoriesRepositoryActionsRuns,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID: RepositoriesRepositoryActionsRunsRunId,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID_HOOKS: RepositoriesRepositoryActionsRunsRunIdHooks,
        PathValues.REPOSITORIES_REPOSITORY_ACTIONS_RUNS_RUN_ID_HOOKS_HOOK_RUN_ID_OUTPUT: RepositoriesRepositoryActionsRunsRunIdHooksHookRunIdOutput,
        PathValues.REPOSITORIES_REPOSITORY_METADATA_META_RANGE_META_RANGE: RepositoriesRepositoryMetadataMetaRangeMetaRange,
        PathValues.REPOSITORIES_REPOSITORY_METADATA_RANGE_RANGE: RepositoriesRepositoryMetadataRangeRange,
        PathValues.REPOSITORIES_REPOSITORY_GC_RULES: RepositoriesRepositoryGcRules,
        PathValues.REPOSITORIES_REPOSITORY_GC_PREPARE_COMMITS: RepositoriesRepositoryGcPrepareCommits,
        PathValues.REPOSITORIES_REPOSITORY_GC_PREPARE_UNCOMMITED: RepositoriesRepositoryGcPrepareUncommited,
        PathValues.REPOSITORIES_REPOSITORY_BRANCH_PROTECTION: RepositoriesRepositoryBranchProtection,
        PathValues.HEALTHCHECK: Healthcheck,
        PathValues.CONFIG_VERSION: ConfigVersion,
        PathValues.CONFIG_STORAGE: ConfigStorage,
        PathValues.CONFIG_GARBAGECOLLECTION: ConfigGarbageCollection,
        PathValues.TEMPLATES_TEMPLATE_LOCATION: TemplatesTemplateLocation,
        PathValues.STATISTICS: Statistics,
    }
)
