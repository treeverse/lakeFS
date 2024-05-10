--[[

Validate the existence of mandatory metadata describing a dataset.
A metadata file should exist either in the same directory as the modified dataset, or in any parent directory.
The closest metadata file would take precedence (i.e. same folder > parent > 2nd parent).

# Example hook definition (_lakefs_actions/validate_dataset_fields.yaml):
name: Validate Dataset Fields
description: Validate the existence of mandatory metadata describing a dataset.
on:
  pre-merge:
    branches:
      - main
hooks:
  - id: validate_datasets
    type: lua
    properties:
      script_path: scripts/dataset_validator.lua
      args:
        prefix: 'datasets/'
        metadata_file_name: dataset_metadata.yaml
        fields:
          - name: contains_pii
            required: true
            type: boolean
          - name: approval_link
            required: true
            type: string
            match_pattern: 'https?:\/\/.*'
          - name: rank
            required: true
            type: number
          - name: department
            type: string
            choices: ['hr', 'it', 'other']
]]

path = require("path")
regexp = require("regexp")
yaml = require("encoding/yaml")

lakefs = require("lakefs")
hook = require("hook")

function is_a_valid_choice(choices, value)
    for _, c in ipairs(choices) do
        if c == value then
            return true
        end
    end
    return false
end

function check_field(field_descriptor, value, filename)
    -- check required but missing
    if value == nil and field_descriptor.required then
        hook.fail(filename .. ": field '" .. field_descriptor.name .. "' is required but no value given")
    end
    -- check type is correct
    if field_descriptor.type ~= nil and type(value) ~= field_descriptor.type then
        hook.fail(filename .. ": field '" .. field_descriptor.name .. "' should be of type " .. field_descriptor.type)
    end
    -- check choices
    if field_descriptor.choices ~= nil and not is_a_valid_choice(field_descriptor.choices, value) then
        hook.fail(filename .. ": field '" .. field_descriptor.name .. "' should be one of '" .. table.concat(field_descriptor.choices, ", ") .. "'")
    end
    -- check pattern
    if field_descriptor.match_pattern ~= nil then
        if value ~= nil and type(value) ~= "string" then
            hook.fail(filename .. ": field " .. field_descriptor.name .. " should be text (got '" .. type(value) .. "') and match pattern '" .. field_descriptor.match_pattern .. "'")
        elseif value ~= nil and not regexp.match(field_descriptor.match_pattern, value) then
            hook.fail(filename .. ": field " .. field_descriptor.name .. " should match pattern '" .. field_descriptor.match_pattern .. "'")
        end
    end
end


-- main flow
after = ""
has_more = true
metadata_files = {}
while has_more do
    local code, resp = lakefs.diff_refs(action.repository_id, action.branch_id, action.source_ref, after, args.prefix)
    if code ~= 200 then
        error("could not diff: " .. resp.message)
    end
    for _, result in pairs(resp.results) do
        print("" .. result.type .. " " .. result.path)
        if result.type == "added" then
            should_check = true
            valid = true
            has_parent = true
            current = result.path
            descriptor_for_file = ""

            -- find nearest metadata file
            while has_parent do
                parsed = path.parse(current)
                if not parsed.parent or parsed.parent == "" then
                    has_parent = false
                    break
                end
                current_descriptor = path.join("/", parsed.parent, args.metadata_file_name)
                -- check if this descriptor has already been cached
                if metadata_files[current_descriptor] then
                    -- cache hit
                    descriptor_for_file = metadata_files[current_descriptor]
                    break

                elseif metadata_files[current_descriptor] == nil then
                    -- cache miss
                    -- attempt to fetch it
                    code, body = lakefs.get_object(action.repository_id, action.source_ref, current_descriptor)
                    if code == 200 then
                        metadata_files[current_descriptor] = yaml.unmarshal(body)
                        descriptor_for_file = current_descriptor
                        break
                    elseif code ~= 404 then
                        error("failed to look up metadata file: '" .. current_descriptor .. "', HTTP " .. tostring(code))
                    else
                        -- indicates this doesn't exist, no need to look it up again
                        metadata_files[current_descriptor] = false
                    end
                end

                current = parsed.parent
            end

            -- check if we found a descriptor
            if descriptor_for_file == "" then
                hook.fail("No dataset metadata found for file: " .. result.path)
            end
        end
    end
    -- pagination
    has_more = resp.pagination.has_more
    after = resp.pagination.next_offset
end

-- now let's review all the metadata files for this commit:
for metadata_filename, metadata_file in pairs(metadata_files) do
    for _, field_descriptor in ipairs(args.fields) do
        check_field(field_descriptor, metadata_file[field_descriptor.name], metadata_filename)
    end
end