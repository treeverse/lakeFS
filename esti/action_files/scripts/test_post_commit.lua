if not action.commit_id or action.commit_id == "" then
    error("Missing commit_id in post-commit hook")
end

if not action.commit.message or action.commit.message == "" then
    error("Missing commit message in post-commit hook")
end

print("Commit ID: " .. action.commit_id)
print("Commit message: " .. action.commit.message)

print("Post-commit validation successful")
