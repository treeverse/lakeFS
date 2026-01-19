if not action.commit_id or action.commit_id == "" then
    error("Missing commit_id in post-merge hook")
end

if not action.commit.message or action.commit.message == "" then
    error("Missing commit message in post-merge hook")
end

if not action.merge_source or action.merge_source == "" then
    error("Missing merge_source in post-merge hook")
end

print("Commit ID: " .. action.commit_id)
print("Commit message: " .. action.commit.message)
print("Merge source: " .. action.merge_source)

print("Post-merge validation successful")
