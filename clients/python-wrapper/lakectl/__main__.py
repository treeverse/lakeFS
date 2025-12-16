
"""
lakectl command line wrapper
"""

import sys
from typing import NoReturn, Optional
from lakefs.__main__ import run


def cli_run(args: Optional[list[str]] = None) -> NoReturn:
    '''
    Main entry point for the lakectl CLI
    '''
    run("lakectl", args)


if __name__ == '__main__':
    cli_run()
