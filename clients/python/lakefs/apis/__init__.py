
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
from lakefs.api.actions_api import ActionsApi
from lakefs.api.auth_api import AuthApi
from lakefs.api.branches_api import BranchesApi
from lakefs.api.commits_api import CommitsApi
from lakefs.api.config_api import ConfigApi
from lakefs.api.health_check_api import HealthCheckApi
from lakefs.api.metadata_api import MetadataApi
from lakefs.api.objects_api import ObjectsApi
from lakefs.api.refs_api import RefsApi
from lakefs.api.repositories_api import RepositoriesApi
from lakefs.api.staging_api import StagingApi
from lakefs.api.tags_api import TagsApi
