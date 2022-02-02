import subprocess
import argparse
import os
import shlex
import time
from datetime import datetime
from string import Template

LAKEFS_ACCESS_KEY = os.getenv('LAKEFS_ACCESS_KEY')
LAKEFS_SECRET_KEY = os.getenv('LAKEFS_SECRET_KEY')
LAKEFS_ENDPOINT = os.getenv('LAKEFS_ENDPOINT')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')

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


def write_status_file(success, status_file_name):
    if success:
        with open(status_file_name,'w') as f:
            f.write(SUCCESS_MSG)
    else:
        with open('errors','r') as errors_file, open(status_file_name,'w') as status_file:
            for line in errors_file:
                if line.startswith('-'):
                    status_file.write("path missing in source: " + line)
                elif line.startswith('+'):
                    status_file.write("path was missing on the destination: " + line)
                elif line.startswith('*'):
                    status_file.write("path was present in source and destination but different: " + line)
                elif line.startswith('!'):
                    status_file.write("there was an error reading or hashing the source or dest: " + line)
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

    if export_diff:
        cmd = ["rclone", "sync", source, args.Dest, "--config=rclone.conf", "--create-empty-src-dirs"]
    else:
        cmd = ["rclone", "copy", source, args.Dest, "--config=rclone.conf", "--create-empty-src-dirs"]

    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.stderr:
        raise Exception("Error while executing rclone command: ", process.stderr)

    # check export and create status file
    if export_diff:
        check_process = subprocess.run(["rclone", "check", source, args.Dest, "--config=rclone.conf", "--combined=errors"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        # if we check the copy command we add the flag --one-way since we only want to check that the source files were copied to the destination
        check_process = subprocess.run(["rclone", "check", source, args.Dest, "--config=rclone.conf", "--combined=errors", "--one-way"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # the command 'rclnoe check --combined' returns files that are identical, as well as errors. So we'll remove the identical files from the errors file
    with open("errors", "r") as input, open("temp", "w") as output:
        for line in input:
            # if line starts with "=" then don't write it in temp file
            if not line.strip("\n").startswith('='):
                output.write(line)
    os.replace('temp', 'errors')

    now = datetime.utcfromtimestamp(time.time())
    if os.stat("errors").st_size == 0:
        status_file_name = "EXPORT_" + reference + "_" + now.strftime("%d-%m-%Y_%H:%M:%S") + "_SUCCESS"
        success = True
    else:
        status_file_name = "EXPORT_" + reference + "_" + now.strftime("%d-%m-%Y_%H:%M:%S") + "_FAILURE"
        success = False

    write_status_file(success, status_file_name)
    upload_status_file = subprocess.run(["rclone", "copy", "./" + status_file_name, args.Dest, "--config=rclone.conf"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

if __name__ == '__main__':
	main()
