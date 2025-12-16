
"""
lakectl command line wrapper
"""

import sys
from typing import NoReturn, Optional
from lakefs.__main__ import (
    run_binary,
    find_or_download_binary
)


def cli_run(args: Optional[list[str]] = None) -> NoReturn:
    '''
    Main entry point for the lakectl CLI
    '''
    args = args or sys.argv[1:]
    lakectl_bin = find_or_download_binary('lakectl')
    run_binary(lakectl_bin, args)


if __name__ == '__main__':
    cli_run()
