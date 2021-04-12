from urllib.parse import urlparse, urlunparse

import lakefs_client
from lakefs_client.apis import ActionsApi, AuthApi, BranchesApi, CommitsApi, ConfigApi, HealthCheckApi, MetadataApi, \
    ObjectsApi, RefsApi, RepositoriesApi, StagingApi, TagsApi

DEFAULT_BASE_URL_PATH = "/api/v1"


class LakeFSClient:
    def __init__(self, configuration=None, header_name=None, header_value=None, cookie=None, pool_threads=1):
        if configuration:
            configuration.host = LakeFSClient._ensure_endpoint(configuration.host)
        self._api = lakefs_client.ApiClient(configuration=configuration, header_name=header_name,
                                            header_value=header_value, cookie=cookie, pool_threads=pool_threads)
        self.actions = ActionsApi(self._api)
        self.auth = AuthApi(self._api)
        self.branches = BranchesApi(self._api)
        self.commits = CommitsApi(self._api)
        self.config = ConfigApi(self._api)
        self.health = HealthCheckApi(self._api)
        self.metadata = MetadataApi(self._api)
        self.objects = ObjectsApi(self._api)
        self.refs = RefsApi(self._api)
        self.repositories = RepositoriesApi(self._api)
        self.staging = StagingApi(self._api)
        self.tags = TagsApi(self._api)

    @staticmethod
    def _ensure_endpoint(endpoint):
        try:
            o = urlparse(endpoint)
            if not o.path or o.path == '/':
                return urlunparse((o.scheme, o.netloc, DEFAULT_BASE_URL_PATH, o.params, o.query, o.fragment))
        except ValueError:
            pass
        return endpoint
