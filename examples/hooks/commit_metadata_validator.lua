--[[
Commit Metadata Validator

Args:
 - Key (string) is the metadata field name to check
 - Value (map<string,string>)  is optional parameters.
     Currently supported: "pattern" whose value is a regexp pattern to match the metadata field value against

Example hook declaration: (_lakefs_actions/pre-commit-metadata-validation.yaml):

name: pre commit metadata field check
on:
pre-merge:
    branches:
    - main
    - stage
hooks:
  - id: check_commit_metadata
    type: lua
    properties:
      script_path: scripts/commit_metadata_validator.lua # location of this script in the repository!
      args:
        notebook_url: {"pattern": "my-jupyter.example.com/.*"}
        spark_version:  {}
]]

regexp = require("regexp")

for k, props in pairs(args) do
    -- let's see that we indeed have this key in out metadata
    local current_value = action.commit.metadata[k]
    if current_value == nil then
        error("missing mandatory metadata field: " .. k)
    end
    if props.pattern and not regexp.match(props.pattern, current_value) then
        error("current value for commit metadata field " .. k .. " does not match pattern: " .. props.pattern .. " - got: " .. current_value)
    end
end
