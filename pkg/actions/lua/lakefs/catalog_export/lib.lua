local extractor = require("lakefs/catalog_export/table_extractor") 
local common = require("lakefs/catalog_export/common") 

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
    lakefs_object_it=common.lakefs_object_it,
    lakefs_hive_partition_it=extractor.lakefs_hive_partition_it,
}
