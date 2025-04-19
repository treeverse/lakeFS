
"""
lakefs.download CLI entry point module.
"""

import sys

from lakefs.__main__ import _download_binaries


def cli_run() -> int:
    '''
    Main entry point for the lakefs.download command line
    '''
    version = None
    if len(sys.argv) > 1:
        version = sys.argv[1]
    _download_binaries(version=version)
    return 0

if __name__ == '__main__':
    sys.exit(cli_run())
