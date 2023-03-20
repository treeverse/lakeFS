# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from lakefs_client.paths.templates_template_location import Api

from lakefs_client.paths import PathValues

path = PathValues.TEMPLATES_TEMPLATE_LOCATION