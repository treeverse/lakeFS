import typing_extensions

from lakefs_client.apis.tags import TagValues
from lakefs_client.apis.tags.actions_api import ActionsApi
from lakefs_client.apis.tags.auth_api import AuthApi
from lakefs_client.apis.tags.branches_api import BranchesApi
from lakefs_client.apis.tags.commits_api import CommitsApi
from lakefs_client.apis.tags.config_api import ConfigApi
from lakefs_client.apis.tags.experimental_api import ExperimentalApi
from lakefs_client.apis.tags.otf_diff_api import OtfDiffApi
from lakefs_client.apis.tags.health_check_api import HealthCheckApi
from lakefs_client.apis.tags.model_import_api import ModelImportApi
from lakefs_client.apis.tags.metadata_api import MetadataApi
from lakefs_client.apis.tags.objects_api import ObjectsApi
from lakefs_client.apis.tags.refs_api import RefsApi
from lakefs_client.apis.tags.repositories_api import RepositoriesApi
from lakefs_client.apis.tags.retention_api import RetentionApi
from lakefs_client.apis.tags.staging_api import StagingApi
from lakefs_client.apis.tags.statistics_api import StatisticsApi
from lakefs_client.apis.tags.tags_api import TagsApi
from lakefs_client.apis.tags.templates_api import TemplatesApi

TagToApi = typing_extensions.TypedDict(
    'TagToApi',
    {
        TagValues.ACTIONS: ActionsApi,
        TagValues.AUTH: AuthApi,
        TagValues.BRANCHES: BranchesApi,
        TagValues.COMMITS: CommitsApi,
        TagValues.CONFIG: ConfigApi,
        TagValues.EXPERIMENTAL: ExperimentalApi,
        TagValues.OTF_DIFF: OtfDiffApi,
        TagValues.HEALTH_CHECK: HealthCheckApi,
        TagValues.IMPORT: ModelImportApi,
        TagValues.METADATA: MetadataApi,
        TagValues.OBJECTS: ObjectsApi,
        TagValues.REFS: RefsApi,
        TagValues.REPOSITORIES: RepositoriesApi,
        TagValues.RETENTION: RetentionApi,
        TagValues.STAGING: StagingApi,
        TagValues.STATISTICS: StatisticsApi,
        TagValues.TAGS: TagsApi,
        TagValues.TEMPLATES: TemplatesApi,
    }
)

tag_to_api = TagToApi(
    {
        TagValues.ACTIONS: ActionsApi,
        TagValues.AUTH: AuthApi,
        TagValues.BRANCHES: BranchesApi,
        TagValues.COMMITS: CommitsApi,
        TagValues.CONFIG: ConfigApi,
        TagValues.EXPERIMENTAL: ExperimentalApi,
        TagValues.OTF_DIFF: OtfDiffApi,
        TagValues.HEALTH_CHECK: HealthCheckApi,
        TagValues.IMPORT: ModelImportApi,
        TagValues.METADATA: MetadataApi,
        TagValues.OBJECTS: ObjectsApi,
        TagValues.REFS: RefsApi,
        TagValues.REPOSITORIES: RepositoriesApi,
        TagValues.RETENTION: RetentionApi,
        TagValues.STAGING: StagingApi,
        TagValues.STATISTICS: StatisticsApi,
        TagValues.TAGS: TagsApi,
        TagValues.TEMPLATES: TemplatesApi,
    }
)
