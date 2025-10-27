
"""
lakectl command line wrapper
"""

import sys
from lakefs.__main__ import (
    run_binary,
    find_or_download_binary
)


def cli_run() -> int:
    '''
    Main entry point for the lakeFS CLI
    '''
    args = sys.argv[1:]
    return run_binary(find_or_download_binary('lakectl'), args)


if __name__ == '__main__':
    sys.exit(cli_run())


if __name__ == '__main__':
    cli_run()
