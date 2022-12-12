--[[
Automatic Symlink Exporter

Args:
 - aws_access_key_id, aws_secret_access_key, aws_region (string): configuration passed to the s3 client that writes symlinks
 - export_bucket (string): bucket to write symlinks to
 - export_path (string): path in the bucket to write symlinks to
 - sources ([]string): lakeFS paths that should be written as symlinks

Example hook declaration: (_lakefs_actions/auto-symlinks.yaml):

name: auto symlink
on:
  post-create-branch:
    branches: ["view-*"]
  post-commit:
    branches: ["view-*"]
hooks:
  - id: symlink_creator
    type: lua
    properties:
      script_path: scripts/s3_hive_manifest_exporter.lua
      args:
        # Export configuration
        aws_access_key_id: "AKIA..."
        aws_secret_access_key: "..."
        aws_region: us-east-1
        export_bucket: oz-repo
        export_path: lakefs_tables
        sources:
          - tables/my-table/
]]

aws = require("aws")
lakefs = require("lakefs")
path = require("path")
path_sep = path.default_separator()

s3 = aws.s3_client(args.aws_access_key_id, args.aws_secret_access_key, args.aws_region)

tag_events = {  ["pre-create-tag"] = true,  ["post-create-tag"] = true }
branch_events = {  ["pre-create-branch"] = true,  ["post-create-branch"] = true }
commit_events = {  ["post-commit"] = true, ["post-merge"] = true }

local current_commit = action.commit_id
local ref
if tag_events[action.event_type] then
    ref = action.tag_id
elseif branch_events[action.event_type] then
    ref = action.branch_id
elseif commit_events[action.event_type] then
    ref = action.branch_id
else
    error("unsupported event type: " .. action.event_type)
end
-- root export path for the current repository
export_path = path.join(path_sep, args.export_path, "repositories", action.repository_id)

for _, location in ipairs(args.sources) do
    location_export_path = path.join(path_sep, export_path, "refs", ref, location)
    start_marker = path.join(path_sep, location_export_path, "_start_commit_id")
    end_marker = path.join(path_sep, location_export_path, "_completed_commit_id")
    -- read start_commit from S3
    start_commit, exists = s3.get_object(args.export_bucket, start_marker)
    if not exists then
        -- no commit marker
        print("no _start_commit_id found for location '" .. location .. "'")
        start_commit = nil
    end
    -- read end_commit from S3
    end_commit, exists = s3.get_object(args.export_bucket, end_marker)
    if not exists then
        -- no commit marker
        print("no _completed_commit_id found for location '" .. location .. "'")
        end_commit = nil
    end

    clean_mode = false
    if (not start_commit or not end_commit) or (start_commit ~= end_commit) then
        -- we need to clean up and start from scratch
        print("going into clean mode for location '" .. location .. "', deleting export path s3://" .. args.export_bucket .. "/" .. location_export_path)
        s3.delete_recursive(args.export_bucket, location_export_path)
        clean_mode = true
    end
    -- write start_commit
    print("writing _start_commit_id: " .. current_commit)
    s3.put_object(args.export_bucket, start_marker, current_commit)

    if clean_mode then
        -- instead of diffing, list the things and gather prefixes
        local after = ""
        local has_more = true
        local current_subloc = ""
        local current_files = {}
        while has_more do
            local code, resp = lakefs.list_objects(action.repository_id, current_commit, after, location, "") -- without delimiter
            if code ~= 200 then
                error("could not list path: " .. location .. ", error: " .. resp.message)
            end
            for _, entry in ipairs(resp.results) do
                p = path.parse(entry.path)
                -- did we move on to the next dir?
                if p.parent ~= current_subloc then
                    -- we moved on to a new directory! let's flush the previous one
                    if #current_files > 0 then
                        symlink_path = path.join(path_sep, location_export_path, current_subloc, "symlink.txt")
                        print("writing symlink file for " .. symlink_path)
                        s3.put_object(args.export_bucket, symlink_path, table.concat(current_files, "\n"))
                    end
                    -- done, updated current dir
                    current_subloc = p.parent
                    current_files = {}
                end
                -- add physical address
                if not path.is_hidden(entry.path) then
                    table.insert(current_files, entry.physical_address)
                end

            end

            -- pagination
            has_more = resp.pagination.has_more
            after = resp.pagination.next_offset
        end
        -- do we have anything left to flush?
        if #current_files > 0 then
            symlink_path = path.join(path_sep, location_export_path, current_subloc, "symlink.txt")
            print("writing symlink file for " .. symlink_path)
            s3.put_object(args.export_bucket, symlink_path, table.concat(current_files, "\n"))
        end
    else
        -- diff start_commit with current_commit
        dirty_locations = {}
        local has_more = true
        local after = ""
        while has_more do
            print("diffing. current commit = " .. current_commit .. ", start commit = " .. start_commit .. ", after = " .. after .. ", location = " .. location)
            local code, resp = lakefs.diff_refs(action.repository_id, start_commit, current_commit, after, location, "") -- recursive
            if code ~= 200 then
                error("could not diff path: " .. location .. ", error: " .. resp.message)
            end
            -- for every modified_prefix
            print("\t got " .. tostring(#resp.results) .. " results, iterating")
            for _, entry in ipairs(resp.results) do
                p = path.parse(entry.path)
                if dirty_locations[#dirty_locations] ~= p.parent then
                    print("adding 'dirty' location: " .. p.parent)
                    table.insert(dirty_locations, p.parent)
                end
            end
            -- pagination
            has_more = resp.pagination.has_more
            after = resp.pagination.next_offset
        end

        -- now, for every dirty location, regenerate its symlink
        for _, subloc in ipairs(dirty_locations) do
            local has_more = true
            local after = ""
            local current_entries = {}
            while has_more do
                local code, resp = lakefs.list_objects(action.repository_id, current_commit, after, subloc, "") -- without delimiter
                if code ~= 200 then
                    error("could not list path: " .. subloc .. ", error: " .. resp.message)
                end
                for _, entry in ipairs(resp.results) do
                    if not path.is_hidden(entry.path) then
                        table.insert(current_entries, entry.physical_address)
                    end
                end
                -- pagination
                has_more = resp.pagination.has_more
                after = resp.pagination.next_offset
            end
            symlink_path = path.join(path_sep, location_export_path, subloc, "symlink.txt")
            if #current_entries == 0 then
                print("removing stale symlink path: " .. symlink_path)
                s3.delete_object(args.export_bucket, symlink_path)
            else
                print("writing symlink path: " .. symlink_path)
                s3.put_object(args.export_bucket, symlink_path, table.concat(current_entries, "\n"))
            end
        end

    end
    -- done with location! write end_marker
    s3.put_object(args.export_bucket, end_marker, current_commit)
    print("done! wrote _completed_commit_id: " .. current_commit)
end
