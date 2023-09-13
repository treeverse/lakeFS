local extractor = require("lakefs/catalogexport/table_extractor") 
local common = require("lakefs/catalogexport/common") 

-- resolve ref value from action.action.event_type 
function ref_from_branch_or_tag() 
    tag_events = {  ["pre-create-tag"] = true,  ["post-create-tag"] = true }
    branch_events = {  ["pre-create-branch"] = true,  ["post-create-branch"] = true }
    commit_events = {  ["post-commit"] = true, ["post-merge"] = true }
    local ref
    if tag_events[action.event_type] then
        return action.tag_id, nil 
    elseif branch_events[action.event_type] or commit_events[action.event_type] then
        return action.branch_id, nil 
    else
        return nil, "unsupported event type: " .. action.event_type
    end
end

return {
    TableExtractor = extractor.TableExtractor,
    ref_from_branch_or_tag=ref_from_branch_or_tag,
}
