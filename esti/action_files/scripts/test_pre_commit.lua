regexp = require("regexp")

for field_name, props in pairs(args) do
    -- Check that we have this field in metadata
    local current_value = action.commit.metadata[field_name]
    if current_value == nil then
        error("missing mandatory metadata field: " .. field_name)
    end

    if props.pattern and not regexp.match(props.pattern, current_value) then
        error("current value for commit metadata field " .. field_name .. " does not match pattern: " .. props.pattern .. " - got: " .. current_value)
    end

    -- Print validated field
    print("  " .. field_name .. ": " .. current_value)
end

print("Pre-commit metadata validation successful")
