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