local SHORT_DIGEST_LEN=6

function short_digest(digest)
    return digest:sub(1, SHORT_DIGEST_LEN)
end 

function lakefs_object_it(lakefs_client, repo_id, commit_id, after, prefix, page_size, delimiter)
    local next_offset = after
    local has_more = true
    return function()
        if not has_more then
            return nil
        end
        local code, resp = lakefs_client.list_objects(repo_id, commit_id, next_offset, prefix, delimiter, page_size)
        if code ~= 200 then
            error("lakeFS: could not list objects in: " .. prefix .. ", error: " .. resp.message)
        end
        local objects = resp.results
        has_more = resp.pagination.has_more
        next_offset = resp.pagination.next_offset
        return objects
    end
end

return {
    lakefs_object_it=lakefs_object_it,
    short_digest=short_digest,
}