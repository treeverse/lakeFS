local lakefs = require "lakefs"
--local delta = require "delta"
--local pathlib = require "path"
--local json = require "encoding/json"
local utils = require "lakefs/catalogexport/internal"

local table_paths = args.table_paths
local ns = args.storage_ns
local repo = args.repository_id
local commit_id = args.commit_id

--[[
 Changes list -> []SingleAction struct

]]

print(args)

for _, path in ipairs(table_paths) do
    local delta_log_path = path.join("/", path, "_delta_log")
    local log_entry_objects = utils.lakefs_object_pager(lakefs, repo, commit_id, "", delta_log_path,"", 50)
    for _, obj in ipairs(log_entry_objects) do
        print(obj)
    end
    ---- lakefs.get_object(repo, commit_id, delta_log_path)
    --local recent_changes = delta.fetch_formatted_changes(table, lakefs_client)
    ----[[
    --{
    --    tableName: string
    --    changes: [
    --        content: string
    --        entry_name: string
    --    ]
    --}
    --]]
    --if options.debug then
    --    print("Recent changes:", recent_changes)
    --end
    --local recent_changes_json = json.unmarshal(recent_changes)
    --local changes = {}
    --for _, change in ipairs(recent_changes) do
    --    local changeJson = json.marshal(change)
    --    changes[changeJson.entry] = changeJson.changes
    --end
    --filesContents[]
    --return {
    --    table_input = table_input
    --}
end
