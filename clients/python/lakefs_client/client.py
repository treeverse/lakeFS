from urllib3.util import parse_url, Url

import lakefs_client
from lakefs_client.apis import ActionsApi, AuthApi, BranchesApi, CommitsApi, ConfigApi, HealthCheckApi, MetadataApi, \
    ObjectsApi, RefsApi, RepositoriesApi, StagingApi, TagsApi

class _WrappedApiClient(lakefs_client.ApiClient):
    """ApiClient that fixes some weirdnesses."""

    # Wrap files_parameters to work with unnamed "files" (e.g. MemIOs).
    def files_parameters(self, files=None):
        if files is not None:
            for (param_name, file_instances) in files.items():
                i = 0
                if file_instances is None:
                    continue
                for file_instance in file_instances:
                    if file_instance is not None and not hasattr(file_instance, 'name'):
                        # Generate a fake name.
                        i += 1
                        file_instance.name = f'{param_name}{i}'
        return super().files_parameters(files)

class LakeFSClient:
    def __init__(self, configuration=None, header_name=None, header_value=None, cookie=None, pool_threads=1):
        if configuration:
            configuration = LakeFSClient._ensure_endpoint(configuration)
        self._api = _WrappedApiClient(configuration=configuration, header_name=header_name,
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
    def _ensure_endpoint(configuration):
        """Normalize lakefs connection endpoint found in configuration's host"""
        if configuration.host:
            try:
                # prefix http scheme if missing
                if not configuration.host.startswith('http://') and not configuration.host.startswith('https://'):
                    configuration.host = 'http://' + configuration.host
                # if 'host' not set any 'path', format the endpoint url with default 'path' based on the generated code
                o = parse_url(configuration.host)
                if not o.path or o.path == '/':
                    settings = configuration.get_host_settings()
                    if settings:
                        base_path = parse_url(settings[0].get('url')).path
                        configuration.host = Url(scheme=o.scheme, auth=o.auth, host=o.host, port=o.port, path=base_path, query=o.query, fragment=o.fragment).url
            except ValueError:
                pass
        return configuration
