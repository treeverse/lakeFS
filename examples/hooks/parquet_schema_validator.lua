
--[[
Parquet schema Validator

Args:
 - locations (list of strings): locations to look for parquet files under
 - sample (boolean): whether reading one new/changed file per directory is enough, or go through all of them

Example hook declaration: (_lakefs_actions/pre-merge-schema-validation.yaml):

name: pre merge format check on main
on:
  pre-merge:
    branches:
      - main
hooks:
  - id: check_formats
    type: lua
    properties:
      script_path: scripts/parquet_schema_validator.lua # location of this script in the repository!
      args:
        sample: true
        column_block_list: ["user_id", "email", "ssn", "private_*"]
        locations:
          - tables/users/
          - tables/sales/
          - prod/
]]


lakefs = require("lakefs")
strings = require("strings")
parquet = require("encoding/parquet")
regexp = require("regexp")
path = require("path")


visited_directories = {}

for _, location in ipairs(args.locations) do
    after = ""
    has_more = true
    need_more = true
    print("checking location: " .. location)
    while has_more do
        print("running diff, location = " .. location .. " after = " .. after)
        local code, resp = lakefs.diff_refs(action.repository_id, action.branch_id, action.source_ref, after, location)
        if code ~= 200 then
            error("could not diff: " .. resp.message)
        end

        for _, result in pairs(resp.results) do
            p = path.parse(result.path)
            print("checking: '" .. result.path .. "'")
            if not args.sample or (p.parent and not visited_directories[p.parent]) then
                if result.path_type == "object" and result.type ~= "removed" then
                    if strings.has_suffix(p.base_name, ".parquet") then
                        -- check it!
                        code, content = lakefs.get_object(action.repository_id, action.source_ref, result.path)
                        if code ~= 200 then
                            error("could not fetch data file: HTTP " .. tostring(code) .. "body:\n" .. content)
                        end
                        schema = parquet.get_schema(content)
                        for _, column in ipairs(schema) do
                            for _, pattern in ipairs(args.column_block_list) do
                                if regexp.match(pattern, column.name) then
                                    error("Column is not allowed: '" .. column.name .. "': type: " .. column.type .. " in path: " .. result.path)
                                end
                            end
                        end
                        print("\t all columns are valid")
                        visited_directories[p.parent] = true
                    end
                end
            else
                print("\t skipping path, directory already sampled")
            end
        end

        -- pagination
        has_more = resp.pagination.has_more
        after = resp.pagination.next_offset
    end
end
