
# flake8: noqa

# Import all APIs into this package.
# If you have many APIs here with many many models used in each API this may
# raise a `RecursionError`.
# In order to avoid this, import only the API that you directly need like:
#
#   from .api.actions_api import ActionsApi
#
# or import this package, but before doing it, use:
#
#   import sys
#   sys.setrecursionlimit(n)

# Import APIs into API package:
from lakefs_client.api.actions_api import ActionsApi
from lakefs_client.api.auth_api import AuthApi
from lakefs_client.api.branches_api import BranchesApi
from lakefs_client.api.commits_api import CommitsApi
from lakefs_client.api.config_api import ConfigApi
from lakefs_client.api.health_check_api import HealthCheckApi
from lakefs_client.api.import_api import ImportApi
from lakefs_client.api.metadata_api import MetadataApi
from lakefs_client.api.objects_api import ObjectsApi
from lakefs_client.api.refs_api import RefsApi
from lakefs_client.api.repositories_api import RepositoriesApi
from lakefs_client.api.retention_api import RetentionApi
from lakefs_client.api.staging_api import StagingApi
from lakefs_client.api.tags_api import TagsApi
from lakefs_client.api.templates_api import TemplatesApi
