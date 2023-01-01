#!/usr/bin/env python3

import argparse
import subprocess
from tempfile import NamedTemporaryFile
import os
import posixpath                # Works for URL pathname manipulation on Windows too
import shlex
import sys
import time
from datetime import datetime
from string import Template

LAKEFS_ACCESS_KEY = os.getenv('LAKEFS_ACCESS_KEY_ID')
LAKEFS_SECRET_KEY = os.getenv('LAKEFS_SECRET_ACCESS_KEY')
LAKEFS_ENDPOINT = os.getenv('LAKEFS_ENDPOINT')
S3_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
S3_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

SUCCESS_MSG = "Export completed successfully!"


def create_rclone_conf_file():
    with open('rclone.conf.template') as f:
        src = Template(f.read())
        vars = {'lakefs_access_key': LAKEFS_ACCESS_KEY, 'lakefs_secret_key': LAKEFS_SECRET_KEY, 'lakefs_endpoint': LAKEFS_ENDPOINT, 's3_access_key': S3_ACCESS_KEY, 's3_secret_key': S3_SECRET_KEY}
        res = src.substitute(vars)
    with open('rclone.conf', 'w+') as f:
        f.write(res)
    return


def set_args():
    parser = argparse.ArgumentParser(description='Process lakeFS export.')

    parser.add_argument('Repo', metavar='repo', type=str, help='name of lakeFS repository')
    parser.add_argument('Dest', metavar='dest', type=str, help='path of destination')
    parser.add_argument('--branch', metavar='branch', type=str, action='store', help='relevant branch in the repository to export from')
    parser.add_argument('--commit_id', metavar='commit', type=str, action='store', help='relevant commit on the repository to export from')
    parser.add_argument('--prev_commit_id', metavar='previous-commit', type=str, action='store', help='if specified, export only the difference between this commit ID and the head of the branch')

    args = parser.parse_args()
    return args


def process_output(dest, src):
    """Process rclone output on file-like object src into file dst.

Rewrite lines to explain more and remove weird logging indicators."""
    for line in src:
        line = line.removesuffix('\n')
        if line.startswith('-'):
            print("path missing in source:", line, file=dest)
        elif line.startswith('+'):
            print("path missing on destination:", line, file=dest)
        elif line.startswith('*'):
            print("path different in source and destination:", line, file=dest)
        elif line.startswith('!'):
            print("error reading or hashing source or dest", line, file=dest)
    return


def main():

    # create rclone configuration file
    create_rclone_conf_file()

    args = set_args()

    reference = ""
    source = "lakefs:" + args.Repo + "/"
    has_branch = (args.branch != None)
    has_commit = (args.commit_id != None)
    export_diff = (args.prev_commit_id != None)
    if has_branch and not has_commit:
        source += args.branch + "/"
        reference = args.branch
    elif not has_branch and has_commit:
        source += args.commit_id + "/"
        reference = args.commit_id
    else:
        raise Exception("Should assign branch or commit, but not both.")

    if has_commit and export_diff:
        raise Exception("Cannot export diff between two commits.")

    now = datetime.utcfromtimestamp(time.time())
    status_file_name_base = f"EXPORT_{reference}_{now.strftime('%d-%m-%Y_%H:%M:%S')}"

    rclone_command = "sync" if export_diff else "copy"
    cmd = ["rclone", rclone_command, source, args.Dest, "--config=rclone.conf", "--create-empty-src-dirs"]

    process = subprocess.run(cmd)
    if process.returncode != 0:
        print(f"rclone {rclone_command} failed", file=sys.stderr)
        exit(1)

    # check export and create status file
    check_cmd = ["rclone", "check", source, args.Dest, "--config=rclone.conf", "--combined=-"]
    # if not export_diff:
    #     # Use the flag --one-way to check a copy command: only need to check
    #     # that the source files were copied to the destination
    #     check_cmd.append("--one-way")

    check_process = subprocess.Popen(check_cmd, stdout=subprocess.PIPE, text=True)

    local_status = NamedTemporaryFile(
        prefix="lakefs_export_status_", suffix=".temp",
        mode="w", delete=False)
    try:
        # local_status cannot be re-opened for read on Windows until we
        # close it for writing.
        process_output(local_status, check_process.stdout)

        # rclone writes until its done, check_process.stdout is closed, so
        # the rclone process ended and wait will not block.
        check_process.wait()

        # Upload status file to destination bucket
        success = check_process.returncode == 0
        if success:
            print(SUCCESS_MSG, file=local_status)
        local_status.close()

        status_file_name = f"{status_file_name_base}_{'SUCCESS' if success else 'FAILURE'}"
        dest_path = posixpath.join(args.Dest, status_file_name)

        upload_process = subprocess.run(["rclone", "copyto", local_status.name, dest_path, "--config=rclone.conf"])
        if upload_process.returncode != 0:
            print("Failed to upload status file", file=sys.stderr)
    finally:
        os.remove(local_status.name)

    if not success:
        exit(1)


if __name__ == '__main__':
    main()
