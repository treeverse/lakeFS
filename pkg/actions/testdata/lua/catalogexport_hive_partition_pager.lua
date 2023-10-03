local hive = require("lakefs/catalogexport/hive")

-- helper function to slice table array
function table.slice(tbl, first, last, step)
    local sliced = {}

    for i = first or 1, last or #tbl, step or 1 do
        sliced[#sliced + 1] = tbl[i]
    end

    return sliced
end

-- lakefs mock package

local lakefs = {
    list_objects = function(repo_id, commit_id, next_offset, prefix, delimiter, page_size)
        local fs = {
            [action.repository_id] = {
                [action.commit_id] = {{
                    physical_address = "s3://bucket/a1/b1/b",
                    path = "letters/a=1/b=1/b.csv"
                }, {
                    physical_address = "s3://bucket/a2/b2/a",
                    path = "letters/a=2/b=2/a.csv"
                }, {
                    physical_address = "s3://bucket/a2/b2/b",
                    path = "letters/a=2/b=2/b.csv"
                }, {
                    physical_address = "",
                    path = "letters/a=2/b=3/_SUCCESS"
                }, {
                    physical_address = "s3://bucket/a2/b3/a",
                    path = "letters/a=2/b=3/a.csv"
                }, {
                    physical_address = "s3://bucket/a3/b4/a",
                    path = "letters/a=3/b=4/a.csv"
                }, {
                    physical_address = "s3://bucket/a3/b4/b",
                    path = "letters/a=3/b=4/b.csv"
                }}
            }
        }
        local all_entries = fs[repo_id][commit_id]
        if next_offset == "" then
            next_offset = 1
        end
        local end_idx = next_offset + page_size
        return 200, {
            results = table.slice(all_entries, next_offset, end_idx),
            pagination = {
                has_more = end_idx < #all_entries,
                next_offset = end_idx + 1
            }
        }
    end
}

local page = 2
local partitions = {"a", "b"}
local prefix = "letters/"
local pager = hive.extract_partition_pager(lakefs, action.repository_id, action.commit_id, prefix, partitions, page)

for part_key, entries in pager do
    print("# partition: " .. part_key)
    for _, entry in ipairs(entries) do
        print("path: " .. entry.path .. " physical: " .. entry.physical_address)
    end
end
