--[[
S3 Branch HEAD exporter
This script exports lakeFS commit IDs to an external location on S3
For each branch, the latest commit ID will be written under:
 s3://<export_bucket>/<export_path>/repositories/<repository_id>/heads/<branch_id>
the content of the file is the commit ID string.

Example configuration to export the heads of all branches:

name: export_all_heads
on:
  post-commit:
    branches:
  post-merge:
    branches:
hooks:
  - id: export_branch_head
    type: lua
    properties:
      script_path: scripts/s3_branch_head_exporter.lua
      args:
        aws_access_key_id: "AKIA.."
        aws_secret_access_key: "..."
        aws_region: us-east-1
        export_bucket: my-external-bucket
        export_path: lakefs-exported-heads
]]

aws = require("aws")
strings = require("strings")

s3 = aws.s3_client(args.aws_access_key_id, args.aws_secret_access_key, args.aws_region)

export_path = args.export_path
if not strings.has_suffix(export_path, "/") then export_path = export_path .. "/" end

s3.put_object(args.export_bucket, export_path .. "repositories/" .. action.repository_id .. "/heads/" .. action.branch_id, action.commit_id)
