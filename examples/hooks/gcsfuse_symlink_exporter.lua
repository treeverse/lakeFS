--[[
GCSFuse Symlink Exporter

Export gcsfuse-compatible symlinks from a path in a lakeFS repository.
gcsfuse (https://github.com/GoogleCloudPlatform/gcsfuse) is used by managed Google Cloud services such as Vertex AI.

Symlinks are supported by writing an empty (0-byte) object with a `gcsfuse_symlink_target` metadata header, with the target
  being the metadata header value.

Note: When mounting using gcsfuse, the --implicit-dirs flag must be used for lakeFS data to appear.

Args:
 - prefix (string): path in lakeFS to export as symlinks
 - destination (string): where in gcs should these symlinks be written to
 - mount.from (string): will be stripped from the physical address of objects when writing the symlink
 - mount.to (string): will be prepended to the physical address of objects when writing the symlink
 - write_current_marker (bool, default = true): if set to false, don't write a "current" symlink that points to the latest commit
 - gcs_credentials_json_string (string): Google Cloud credentials to use when writing to symlink destination


Example hook declaration: (_lakefs_actions/export_images.yaml):
name: export_images
on:
  post-commit:
  branches:
    - main
  hooks:
    - id: gcsfuse_export_images
      type: lua
      properties:
        script_path: scripts/export_gcs_fuse.lua
        args:
          prefix: "datasets/images/"
          destination: "gs://my-bucket/exports/my-repo/"
          mount:
            from: "gs://my-bucket/repos/my-repo/"
            to: "/gcs/my-bucket/repos/my-repo/"
          gcs_credentials_json_string: |
            {
              "client_id": "...",
              "client_secret": "...",
              "refresh_token": "...",
              "type": "..."
            }
]]

gcloud = require("gcloud")
lakefs = require("lakefs")
path = require("path")

-- initialize client
print("initializing GS client")
gs = gcloud.gs_client(args.gcs_credentials_json_string)

-- get the current commit ID and ref
local current_commit = action.commit_id
tag_events = {  ["pre-create-tag"] = true,  ["post-create-tag"] = true }
branch_events = {  ["pre-create-branch"] = true,  ["post-create-branch"] = true, ["post-commit"] = true, ["post-merge"] = true }
local ref
local ref_type
if tag_events[action.event_type] then
    ref = action.tag_id
    ref_type = "tags"
elseif branch_events[action.event_type] then
    ref = action.branch_id
    ref_type = "branches"
else
    error("unsupported event type: " .. action.event_type)
end
print("using ref_type = " .. ref_type .. ", ref = " .. ref)

local total = 0
local after = ""
local has_more = true
local out = path.join("/", args.destination, "commits", current_commit)

while has_more do
    local code, resp = lakefs.list_objects(action.repository_id, current_commit, after, args.prefix, "") -- without delimiter
    if code ~= 200 then
        error("could not list path: " .. args.prefix .. ", error: " .. resp.message)
    end
    for _, entry in ipairs(resp.results) do
        total = total + 1
        gs.write_fuse_symlink(
                entry.physical_address,
                path.join("/", out, entry.path),
                {["from"] = args.mount.from, ["to"] = args.mount.to})
    end
    -- pagination
    has_more = resp.pagination.has_more
    after = resp.pagination.next_offset
end

print("-- done writing object symlinks (" .. total .. " total symlinks created) --")

if args["write_current_marker"] ~= false then
    local marker = path.join("/", args.destination, ref_type, ref)
    gs.write_fuse_symlink("../commits/" .. current_commit, marker, {})
end
