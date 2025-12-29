
"""
lakefs.quickstart CLI entry point module.
"""

from lakefs.__main__ import cli_run


if __name__ == '__main__':
    cli_run(['run', '--quickstart'])
