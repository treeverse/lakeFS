import warnings

from urllib3.util import parse_url, Url

import lakefs_client.apis
from lakefs_client.api import actions_api
from lakefs_client.api import auth_api
from lakefs_client.api import branches_api
from lakefs_client.api import commits_api
from lakefs_client.api import config_api
from lakefs_client.api import experimental_api
from lakefs_client.api import external_api
from lakefs_client.api import health_check_api
from lakefs_client.api import import_api
from lakefs_client.api import internal_api
from lakefs_client.api import metadata_api
from lakefs_client.api import objects_api
from lakefs_client.api import refs_api
from lakefs_client.api import repositories_api
from lakefs_client.api import staging_api
from lakefs_client.api import tags_api


class _WrappedApiClient(lakefs_client.ApiClient):
    """ApiClient that fixes some weirdness"""

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
        configuration = LakeFSClient._ensure_endpoint(configuration)
        self._api = _WrappedApiClient(configuration=configuration, header_name=header_name,
                                          header_value=header_value, cookie=cookie, pool_threads=pool_threads)
        self.actions_api = actions_api.ActionsApi(self._api)
        self.auth_api = auth_api.AuthApi(self._api)
        self.branches_api = branches_api.BranchesApi(self._api)
        self.commits_api = commits_api.CommitsApi(self._api)
        self.config_api = config_api.ConfigApi(self._api)
        self.experimental_api = experimental_api.ExperimentalApi(self._api)
        self.external_api = external_api.ExternalApi(self._api)
        self.health_check_api = health_check_api.HealthCheckApi(self._api)
        self.import_api = import_api.ImportApi(self._api)
        self.internal_api = internal_api.InternalApi(self._api)
        self.metadata_api = metadata_api.MetadataApi(self._api)
        self.objects_api = objects_api.ObjectsApi(self._api)
        self.refs_api = refs_api.RefsApi(self._api)
        self.repositories_api = repositories_api.RepositoriesApi(self._api)
        self.staging_api = staging_api.StagingApi(self._api)
        self.tags_api = tags_api.TagsApi(self._api)

    @staticmethod
    def _ensure_endpoint(configuration):
        """Normalize lakefs connection endpoint found in configuration's host"""
        if not configuration or not configuration.host:
            return configuration
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
                    configuration.host = Url(scheme=o.scheme, auth=o.auth, host=o.host, port=o.port,
                                             path=base_path, query=o.query, fragment=o.fragment).url
        except ValueError:
            pass
        return configuration

    @property
    def actions(self):
        warnings.warn("Deprecated property. Use actions_api instead.", DeprecationWarning)
        return self.actions_api

    @property
    def auth(self):
        warnings.warn("Deprecated property. Use auth_api instead.", DeprecationWarning)
        return self.auth_api

    @property
    def branches(self):
        warnings.warn("Deprecated property. Use branches_api instead.", DeprecationWarning)
        return self.branches_api

    @property
    def commits(self):
        warnings.warn("Deprecated property. Use commits_api instead.", DeprecationWarning)
        return self.commits_api

    @property
    def config(self):
        warnings.warn("Deprecated property. Use config_api instead.", DeprecationWarning)
        return self.config_api

    @property
    def experimental(self):
        warnings.warn("Deprecated property. Use experimental_api instead.", DeprecationWarning)
        return self.experimental_api

    @property
    def health_check(self):
        warnings.warn("Deprecated property. Use health_check_api instead.", DeprecationWarning)
        return self.health_check_api

    @property
    def metadata(self):
        warnings.warn("Deprecated property. Use metadata_api instead.", DeprecationWarning)
        return self.metadata_api

    @property
    def objects(self):
        warnings.warn("Deprecated property. Use objects_api instead.", DeprecationWarning)
        return self.objects_api

    @property
    def otf_diff(self):
        warnings.warn("Deprecated property. Use otf_diff_api instead.", DeprecationWarning)
        return self.otf_diff_api

    @property
    def refs(self):
        warnings.warn("Deprecated property. Use refs_api instead.", DeprecationWarning)
        return self.refs_api

    @property
    def repositories(self):
        warnings.warn("Deprecated property. Use repositories_api instead.", DeprecationWarning)
        return self.repositories_api

    @property
    def retention(self):
        warnings.warn("Deprecated property. Use retention_api instead.", DeprecationWarning)
        return self.retention_api

    @property
    def staging(self):
        warnings.warn("Deprecated property. Use staging_api instead.", DeprecationWarning)
        return self.staging_api

    @property
    def statistics(self):
        warnings.warn("Deprecated property. Use statistics_api instead.", DeprecationWarning)
        return self.statistics_api

    @property
    def tags(self):
        warnings.warn("Deprecated property. Use tags_api instead.", DeprecationWarning)
        return self.tags_api

    @property
    def templates(self):
        warnings.warn("Deprecated property. Use templates_api instead.", DeprecationWarning)
        return self.templates_api
