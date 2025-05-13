from typing import Any

from setuptools import setup, find_packages, Command, Distribution, glob
import os
import shutil

NAME = "lakefs"
VERSION = "0.10.0"
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
    "pandas ~= 2.1.4",
    "pyarrow ~= 14.0.1",
    "pillow ~= 10.3.0"
]

with open('README.md') as f:
    long_description = f.read()

# to use the `clean` command, run: python -m build clean --all
class CleanCommand(Command):
    """Custom clean command to tidy up the project root with selective options."""
    user_options = [
        ('all', 'a', "Remove all build artifacts (default)"),
        ('build', 'b', "Remove only build directory"),
        ('dist', 'd', "Remove only dist directory"),
        ('eggs', 'e', "Remove only .egg and .egg-info directories"),
        ('cache', 'c', "Remove only Python cache files (__pycache__, .pyc, etc.)"),
        ('tests', 't', "Remove only test artifacts (.pytest_cache, .coverage, etc.)"),
        ('dry-run', 'n', "Don't actually remove files; just show what would be done"),
    ]

    def initialize_options(self):
        self.all = False
        self.build = False
        self.dist = False
        self.eggs = False
        self.cache = False
        self.tests = False
        self.dry_run = False

    def finalize_options(self):
        # If no specific option is selected, default to --all
        if not any([self.build, self.dist, self.eggs, self.cache, self.tests]):
            self.all = True

    def run(self):
        """Remove build artifacts based on selected options."""
        directories = {
            'build': ['./build'],
            'dist': ['./dist'],
            'eggs': ['./*.egg-info', './*.egg'],
            'cache': ['./__pycache__', './**/__pycache__', './**/*.pyc', './**/*.pyo', './**/*.pyd'],
            'tests': ['./.pytest_cache', './.coverage', './htmlcov', './.tox'],
        }

        to_clean = []
        if self.all:
            to_clean = [path for paths in directories.values() for path in paths]
        else:
            if self.build:
                to_clean.extend(directories['build'])
            if self.dist:
                to_clean.extend(directories['dist'])
            if self.eggs:
                to_clean.extend(directories['eggs'])
            if self.cache:
                to_clean.extend(directories['cache'])
            if self.tests:
                to_clean.extend(directories['tests'])

        for directory in to_clean:
            if '*' in directory:
                for path in glob.glob(directory):
                    self._remove_path(path)
            else:
                self._remove_path(directory)

        print(f"Clean {'(dry run)' if self.dry_run else ''} completed.")

    def _remove_path(self, path):
        """Remove a file or directory with dry run support."""
        if not os.path.exists(path):
            return

        if self.dry_run:
            action = "Would remove"
        else:
            action = "Removing"

        if os.path.isdir(path) and not os.path.islink(path):
            print(f"{action} directory: {path}")
            if not self.dry_run:
                shutil.rmtree(path, ignore_errors=True)
        else:
            print(f"{action} file: {path}")
            if not self.dry_run:
                os.remove(path)

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
    # Install using: `pip install "lakefs-<version>-py3-none-any.whl[all,aws-iam]"` (or just one of them)
    extras_require={
        "all": ["boto3 >= 1.26.0"],
        "aws-iam": ["boto3 >= 1.26.0"],
    },
    cmdclass={
        'clean': CleanCommand,
    },
)
