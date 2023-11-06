from setuptools import setup, find_packages

NAME = "pylotl"
VERSION = "0.1.0-alpha"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

PYTHON_REQUIRES = ">=3.7"
REQUIRES = [
    "urllib3 >= 1.25.3, < 2.1.0",
    "python-dateutil",
    "pydantic >= 1.10.5, < 2",
    "aenum"
]

# TODO: autogenerate docs
# with open('README.md') as f:
#     import re
#
#     # replace relative links with links to the docs:
#     doc_version = '.'.join(f'v{VERSION}'.split('.')[:2])
#     long_description = re.sub(r'(\[[*a-zA-Z_]*]\()docs/([A-Za-z0-9]*)\.md',
#                               rf'\1https://pydocs-sdk.lakefs.io/{doc_version}/docs/\2.html',
#                               f.read())

setup(
    name=NAME,
    version=VERSION,
    description="lakeFS API",
    author="Treeverse",
    author_email="services@treeverse.io",
    url="https://github.com/treeverse/lakeFS/tree/master/clients/python-wrapper",
    keywords=["OpenAPI", "OpenAPI-Generator", "lakeFS API", "Python Wrapper"],
    python_requires=">=3.10",
    install_requires=REQUIRES,
    tests_require={
        "dev": ["pytest ~= 7.4.3", "pytest-mock ~= 3.12.0"]},
    packages=find_packages(exclude=["test", "tests"]),
    include_package_data=True,
    license="Apache 2.0",
    # long_description=long_description,
    long_description_content_type='text/markdown'
)
