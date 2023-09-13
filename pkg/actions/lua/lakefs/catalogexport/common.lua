local SHORT_DIGEST_LEN=6

function short_digest(digest)
    return digest:sub(1, SHORT_DIGEST_LEN)
end 

function lakefs_paginiated_api(api_call, after)
    local next_offset = after
    local has_more = true
    return function()
        if not has_more then
            return nil
        end
        local code, resp = api_call(next_offset)
        if code < 200 or code >= 300 then
            error("lakeFS: api return non-2xx" .. code)
        end
        has_more = resp.pagination.has_more
        next_offset = resp.pagination.next_offset
        return resp.results
    end
end

function lakefs_object_pager(lakefs_client, repo_id, commit_id, after, prefix, page_size, delimiter)
    return lakefs_paginiated_api(function(next_offset)
        return lakefs_client.list_objects(repo_id, commit_id, next_offset, prefix, delimiter, page_size)
    end, after)
end 

return {
    lakefs_object_pager=lakefs_object_pager,
    short_digest=short_digest,
}