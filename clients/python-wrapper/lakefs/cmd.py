"""
This module provides functionality to find and run the lakeFS and lakectl binaries.

The module provides the following functions:
- find_or_download_binary: Find the lakeFS binary in 
    $PATH or ~/.lakefs/bin, or download it if not found.
- run_binary: Run the lakeFS binary with the provided arguments.
"""

import os
import subprocess
import sys
import shutil
import platform
import tarfile
from typing import Optional
import io
import urllib.request
import json
from collections import namedtuple
import zipfile

try:
    import tqdm
except ImportError:
    tqdm = None


BINARY_DOWNLOAD_DIR = '~/.lakefs/bin'
BINARY_DOWNLOAD_URL = 'https://github.com/treeverse/lakeFS/releases/download/'
BINARY_LATEST_RELEASE_URL = 'https://api.github.com/repos/treeverse/lakeFS/releases/latest'

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


def _is_python_script(file_path: str) -> bool:
    '''
    Check if the file is a Python script by looking for a shebang line.
    '''
    platform_info =_get_platform_info()
    with open(file_path, 'rb') as f:
        if platform_info.system == 'windows':
            # on windows, Python places an executable EXE file.
            # it contains the python snippet at the end of the file
            f.seek(-4096, io.SEEK_END)  # last 4k
            return b'__main__.py' in f.read(4096)
        # on unix, the lakeFS binary is a Python script
        first_line = f.readline()
        return first_line.startswith(b'#!') and b'python' in first_line.lower()


def _find_binary(binary_name: str) -> Optional[str]:
    '''
    Find the binary in PATH or ~/.lakefs/bin, skipping Python scripts.
    Returns the path to the binary or None if not found.
    '''
    home_bin_path = os.path.expanduser(f'{BINARY_DOWNLOAD_DIR}/{binary_name}')
    # Check if the binary is in the PATH
    binary_path = shutil.which(binary_name)
    # If the found binary is a Python script, find the next occurrence
    platform_info =_get_platform_info()
    if platform_info.system == 'windows':
        binary_name = f'{binary_name}.exe'
        home_bin_path = os.path.expanduser(f'{BINARY_DOWNLOAD_DIR}/{binary_name}')
    if binary_path and _is_python_script(binary_path):
        # Find the next occurrence of the binary in PATH
        for path in os.environ.get('PATH', '').split(os.pathsep):
            candidate = os.path.join(path, binary_name)
            if os.path.isfile(candidate) and not _is_python_script(candidate):
                binary_path = candidate
                break
        else:
            binary_path = None
    # If not found in PATH, check the home directory path
    if not binary_path and os.path.isfile(home_bin_path):
        binary_path = home_bin_path
    return binary_path


def _get_latest_version() -> str:
    '''
    Get the latest lakeFS release version from GitHub API
    '''
    try:
        with urllib.request.urlopen(BINARY_LATEST_RELEASE_URL) as response:
            data = json.loads(response.read().decode())
            return data['tag_name'].lstrip('v')  # Remove 'v' prefix from version
    except (urllib.error.URLError, json.JSONDecodeError, KeyError) as e:
        raise RuntimeError(f"Error getting latest lakeFS version: {e}") from e


class FakeProgressBar:
    """A no-op progress bar used when tqdm is not available."""
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        pass

    def update(self, _size: int):
        """Update the progress bar with the given size (no-op)."""


def _progress(total_size: int):
    if tqdm:
        return tqdm.tqdm(total=total_size, unit='iB', unit_scale=True)
    return FakeProgressBar()


def _download_binary(binary_name: str) -> str:
    '''
    Download the binary from lakeFS releases
    Returns the path to the binary
    '''
    version = _get_latest_version()
    platform_info = _get_platform_info()
    compression = 'tar.gz'
    if platform_info.system == 'windows':
        compression = 'zip'
    # Construct download URL
    url = (f"{BINARY_DOWNLOAD_URL}"
           f"v{version}/lakeFS_{version}_{platform_info.system}_"
           f"{platform_info.machine}.{compression}")
    content = io.BytesIO()
    try:
        print(f"Downloading {binary_name} v{version} for "
              f"{platform_info.system}/{platform_info.machine}...")
        req = urllib.request.Request(url, headers={'User-Agent': 'lakeFS SDK Downloader'})
        with urllib.request.urlopen(req) as response:
            total_size = int(response.headers.get('Content-Length', 0))
            block_size = 1024
            # Create ~/.lakefs/bin directory if it doesn't exist
            bin_dir = os.path.expanduser(BINARY_DOWNLOAD_DIR)
            os.makedirs(bin_dir, exist_ok=True)
            # Download with progress bar
            with _progress(total_size) as progress_bar:
                while True:
                    data = response.read(block_size)
                    if not data:
                        break
                    size = content.write(data)
                    progress_bar.update(size)
                content.seek(0)
    except urllib.error.URLError as e:
        raise RuntimeError(f"Error downloading {binary_name}: {e}") from e

    # Extract the binary
    if platform_info.system == 'windows':
        binary_name = f'{binary_name}.exe'
    if compression == 'zip':
        try:
            with zipfile.ZipFile(content, 'r') as zip_ref:
                zip_ref.extract(binary_name, bin_dir)
                binary_path = os.path.join(bin_dir, binary_name)
                return binary_path
        except (zipfile.BadZipFile, OSError) as e:
            raise RuntimeError(f"Error extracting {binary_name}: {e}") from e
    try:
        with tarfile.open(fileobj=content, mode='r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith(binary_name):
                    member.name = os.path.basename(member.name)
                    tar.extract(member, bin_dir)
                    binary_path = os.path.join(bin_dir, binary_name)
                    if platform_info.system != 'windows':
                        # Only set executable permission on Unix-like systems
                        os.chmod(binary_path, 0o755)
                    return binary_path
    except (tarfile.TarError, OSError) as e:
        raise RuntimeError(f"Error extracting {binary_name}: {e}") from e


def find_or_download_binary(binary_name: str) -> str:
    '''
    Find the binary in PATH or ~/.lakefs/bin, 
    or download it if not found
    Returns the path to the binary
    '''
    binary_path = _find_binary(binary_name)
    if not binary_path:
        binary_path = _download_binary(binary_name)
    return binary_path


def run_binary(binary_path: str):
    '''
    Run the binary with the provided arguments.
    '''
    if not binary_path:
        raise RuntimeError("binary not found")
    binary_name = os.path.basename(binary_path)
    try:
        print(f'running {binary_path}...')
        proc = subprocess.run(
            [binary_path] + sys.argv[1:], check=False, env=os.environ)
        return proc.returncode
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error executing {binary_name}: {e}") from e
