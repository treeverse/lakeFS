from setuptools import setup, find_packages

NAME = "lakefs"
VERSION = "0.13.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

PYTHON_REQUIRES = ">=3.9"
REQUIRES = [
    "lakefs-sdk >= 1.50, < 2",
    "pyyaml ~= 6.0.1",
]
TEST_REQUIRES = [
    "pytest ~= 7.4.3",
    "pytest-datafiles ~= 3.0.0",
    "pandas >= 2.1.4,<3",
    "pyarrow >= 14.0.1,<20",
    "pillow >= 10.3.0,<12"
]

with open('README.md') as f:
    long_description = f.read()
setup(
    name=NAME,
    version=VERSION,
    description="lakeFS Python SDK Wrapper",
    author="Treeverse",
    author_email="services@treeverse.io",
    url="https://github.com/treeverse/lakeFS/tree/master/clients/python-wrapper",
    keywords=["OpenAPI", "OpenAPI-Generator", "lakeFS API", "Python Wrapper"],
    python_requires=">=3.9",
    install_requires=REQUIRES,
    tests_require=TEST_REQUIRES,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    license="Apache 2.0",
    long_description=long_description,
    long_description_content_type='text/markdown',
    extras_require={
        # Install using: `pip install "lakefs-<version>-py3-none-any.whl[all,aws-iam]"`
        "all": ["boto3 >= 1.26.0"],
        "aws-iam": ["boto3 >= 1.26.0"],
    },
    package_data={"lakefs": ["py.typed"]},
)
