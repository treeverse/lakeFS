
"""
lakeFS CLI entry point module.

This module serves as the main entry point for the lakeFS command-line interface.
It handles command-line argument parsing and execution of the CLI commands.
"""

import sys
import platform
import io
import urllib.request
import urllib.error
import zipfile
import tarfile
import os
import shutil
import subprocess
from collections import namedtuple
from typing import Optional

import lakefs_sdk


BINARY_DOWNLOAD_URL = 'https://github.com/treeverse/lakeFS/releases/download/'


_PlatformInfo = namedtuple('PlatformInfo', ['system', 'machine'])


def _get_platform_info() -> _PlatformInfo:
    '''
    Get platform information for binary download
    '''
    system = platform.system().lower()
    machine = platform.machine().lower()
    # Map platform to lakeFS release format
    if machine in ('amd64', 'x86_64', 'i686'):
        machine = 'x86_64'
    elif machine in ('arm64', 'aarch64'):
        machine = 'arm64'
    else:
        raise ValueError(f"Unsupported platform: {machine}")
    if system not in ('darwin', 'linux', 'windows'):
        raise ValueError(f"Unsupported OS: {system}")
    return _PlatformInfo(system, machine)


def _determine_binary_path() -> str:
    '''
    Determine the correct path to place binaries
    '''
    for bin_relative_dir in ('bin', 'Scripts'):
        python_bin_dir = os.path.join(sys.exec_prefix, bin_relative_dir)
        if os.path.isdir(python_bin_dir) and os.access(python_bin_dir, os.W_OK):
            return python_bin_dir  # Most likely a virtualenv
    # fallback to ~/.lakefs/bin
    return os.path.expanduser('~/.lakefs/bin')


def _get_cli_version() -> str:
    '''
    Get the lakeFS release version
    '''
    return lakefs_sdk.__version__


def _download_file(url: str) -> io.BytesIO:
    '''
    Download a file from the given URL and return it as a BytesIO object
    '''
    content = io.BytesIO()
    try:
        req = urllib.request.Request(
            url, headers={'User-Agent': 'lakeFS SDK Downloader'})
        with urllib.request.urlopen(req) as response:
            data = response.read()
            content.write(data)
            content.seek(0)
    except urllib.error.URLError as e:
        raise RuntimeError(f"Error downloading file: {e}") from e
    return content


def _extract_binaries(
        content: io.BytesIO,
        target_dir: str,
        platform_info: _PlatformInfo):
    '''
    Extract the binary from the downloaded content
    '''
    if platform_info.system == 'windows':
        try:
            with zipfile.ZipFile(content, 'r') as zip_ref:
                zip_ref.extract('lakefs.exe', target_dir)
                zip_ref.extract('lakectl.exe', target_dir)
        except (zipfile.BadZipFile, OSError) as e:
            raise RuntimeError(f"Error extracting lakefs binaries: {e}") from e
        return
    try:
        with tarfile.open(fileobj=content, mode='r:gz') as tar:
            tar.extract('lakefs', target_dir, filter='data')
            tar.extract('lakectl', target_dir, filter='data')
            os.chmod(os.path.join(target_dir, 'lakefs'), 0o755)
            os.chmod(os.path.join(target_dir, 'lakectl'), 0o755)
    except (tarfile.TarError, OSError) as e:
        raise RuntimeError(f"Error extracting lakefs binaries: {e}") from e


def _download_binaries(version: Optional[str] = None):
    '''
    Download the binary from lakeFS releases
    Returns the path to the binary
    '''
    if not version:
        version = _get_cli_version()
    platform_info = _get_platform_info()
    compression = 'zip' if platform_info.system == 'windows' else 'tar.gz'
    url = (f"{BINARY_DOWNLOAD_URL}"
           f"v{version}/lakeFS_{version}_{platform_info.system}_"
           f"{platform_info.machine}.{compression}")
    print(f"Downloading lakeFS v{version} for "
          f"{platform_info.system}/{platform_info.machine}...")
    bin_dir = _determine_binary_path()
    os.makedirs(bin_dir, exist_ok=True)
    content = _download_file(url)
    _extract_binaries(content, bin_dir, platform_info)
    print(f"lakefs and lakectl binaries successfully downloaded to: {bin_dir}")


def _find_binary(binary_name: str) -> Optional[str]:
    '''
    Find the requested binary in the following by order of preference:
     - $PATH
     - {sys.exec_prefix}/bin
     - {sys.exec_prefix}/Scripts (venv on windows)
     - ~/.lakefs/bin
    '''
    bin_directory = _determine_binary_path()
    bin_path = os.path.expanduser(f'{bin_directory}/{binary_name}')
    # Check if the binary is in the PATH
    binary_path = shutil.which(binary_name)
    if binary_path:
        return binary_path
    platform_info = _get_platform_info()
    if platform_info.system == 'windows':
        bin_path = bin_path + '.exe'
    # If not found in PATH, check the home directory path
    if os.path.isfile(bin_path):
        binary_path = bin_path
    return binary_path


def run_binary(binary_path: str, args: list[str]):
    '''
    Run the binary with the provided arguments.
    '''
    if not binary_path:
        raise RuntimeError("binary not found")
    binary_name = os.path.basename(binary_path)
    try:
        print(f'running {binary_path}...')
        proc = subprocess.run(
            [binary_path] + args, check=False, env=os.environ)
        return proc.returncode
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error executing {binary_name}: {e}") from e


def find_or_download_binary(binary_name: str) -> str:
    '''
    Find the binary in PATH or ~/.lakefs/bin, 
    or download it if not found
    Returns the path to the binary
    '''
    binary_path = _find_binary(binary_name)
    if not binary_path:
        _download_binaries()
        binary_path = _find_binary(binary_name)
    return binary_path


def cli_run() -> int:
    '''
    Main entry point for the lakeFS CLI
    '''
    args = sys.argv[1:]
    return run_binary(find_or_download_binary('lakefs'), args)


if __name__ == '__main__':
    sys.exit(cli_run())
