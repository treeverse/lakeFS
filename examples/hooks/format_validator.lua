--[[
Format Validator

Args:
 - ignore_hidden (boolean): whether or not to disregard objects that are hidden (begin with a "_" or part of a directory that begins with a "_")
 - allow_list (list of strings): allowed suffixes. For example, ["parquet", "orc"]

Example hook declaration: (_lakefs_actions/pre-merge-format-validation.yaml):

name: pre merge format check on main
on:
pre-merge:
  branches:
    - main
hooks:
  - id: check_formats
    type: lua
    properties:
    script_path: scripts/format_validator.lua # location of this script in the repository!
    args:
        allow_list: ["parquet", "orc", "log"]
        ignore_hidden:  true
]]

lakefs = require("lakefs")
strings = require("strings")


forbidden_paths = {}
has_more = true
after = ""
while has_more do
  local code, resp = lakefs.diff_refs(action.repository_id, action.branch_id, action.source_ref, after)
  if code ~= 200 then
    error("could not diff: " .. resp.message)
  end

  for _, result in pairs(resp.results) do
    if result.path_type == "object" and result.type == "added" then
        should_check = true
        valid = false
        path_parts = strings.split(result.path, "/")
        base_name = path_parts[#path_parts]

        -- hidden in this case is any file that begins with "_"
        -- or that belongs to a directory that begins with foo
        if args.ignore_hidden then
            for _, path_part in ipairs(path_parts) do
                if strings.has_prefix(path_part, "_") then
                    should_check = false
                    break
                end
            end
        end

        -- check if this file is allowed
        if should_check then
            for _, ext in ipairs(args.allow_list) do
                if strings.has_suffix(base_name, ext) then
                    valid = true
                    break
                end
            end
            if not valid then
                table.insert(forbidden_paths, result.path)
            end
        end
    end
  end

  -- pagination
  has_more = resp.pagination.has_more
  after = resp.pagination.next_offset
end

if #forbidden_paths > 0 then
    print("Found forbidden paths:")
    for _, p in ipairs(forbidden_paths) do
        print(p)
    end
    error("forbidden paths found")
end
