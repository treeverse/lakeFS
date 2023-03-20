# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from lakefs_client.apis.tag_to_api import tag_to_api

import enum


class TagValues(str, enum.Enum):
    ACTIONS = "actions"
    AUTH = "auth"
    BRANCHES = "branches"
    COMMITS = "commits"
    CONFIG = "config"
    EXPERIMENTAL = "experimental"
    OTF_DIFF = "otf diff"
    HEALTH_CHECK = "healthCheck"
    IMPORT = "import"
    METADATA = "metadata"
    OBJECTS = "objects"
    REFS = "refs"
    REPOSITORIES = "repositories"
    RETENTION = "retention"
    STAGING = "staging"
    STATISTICS = "statistics"
    TAGS = "tags"
    TEMPLATES = "templates"
