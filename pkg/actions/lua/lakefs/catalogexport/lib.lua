local extractor = require("lakefs/catalogexport/table_extractor")
local common = require("lakefs/catalogexport/common")

-- resolve ref value from action global
function ref_from_branch_or_tag(action_info)
    event = action_info.event_type
    if event == "pre-create-tag" or event == "post-create-tag" then
        return action_info.tag_id
    elseif event == "pre-create-branch" or event == "post-create-branch" or "post-commit" or "post-merge" then
        return action_info.branch_id
    else
        error("unsupported event type: " .. action_info.event_type)
    end
end

return {
    TableExtractor = extractor.TableExtractor,
    ref_from_branch_or_tag = ref_from_branch_or_tag
}
