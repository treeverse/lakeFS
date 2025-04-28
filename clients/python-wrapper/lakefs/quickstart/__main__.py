
"""
lakefs.quickstart CLI entry point module.
"""

import sys

from lakefs.__main__ import find_or_download_binary, run_binary


def cli_run() -> int:
    '''
    Main entry point for the lakefs.quickstart command line
    '''
    run_binary(find_or_download_binary('lakefs'), ['run', '--quickstart'])
    return 0

if __name__ == '__main__':
    sys.exit(cli_run())
