local hive = require("lakefs/catalogexport/hive")
local strings = require("strings")

-- lakefs mock package: lists the given (sorted) paths under prefix
local function mock_client(paths)
    return {
        list_objects = function(repo_id, commit_id, after, prefix, delimiter, page_size)
            local matched = {}
            for _, p in ipairs(paths) do
                if strings.has_prefix(p, prefix) then
                    table.insert(matched, {
                        physical_address = "s3://bucket/" .. p,
                        path = p,
                        size_bytes = 1
                    })
                end
            end
            local first = (after == "") and 1 or after
            local last = math.min(first + page_size - 1, #matched)
            local results = {}
            for i = first, last do
                table.insert(results, matched[i])
            end
            return 200, {
                results = results,
                pagination = {
                    has_more = last < #matched,
                    next_offset = last + 1
                }
            }
        end
    }
end

-- table under a "_" path: only segments below the table path are checked for "_"
local staging = {
    "_staging/ev/_SUCCESS",
    "_staging/ev/_tmp/dt=2024-01-01/part-9.parquet",
    "_staging/ev/dt=2024-01-01/_SUCCESS",
    "_staging/ev/dt=2024-01-01/part-0.parquet",
    "_staging/ev/dt=2024-01-02/.part-1.parquet.crc",
    "_staging/ev/dt=2024-01-02/_temporary/0/part-8.parquet",
    "_staging/ev/dt=2024-01-02/part-1.parquet",
    "_staging/ev2/dt=2024-01-03/part-0.parquet",
}

local cases = {
    {base = "_staging/ev", cols = {"dt"}, paths = staging},
    {base = "_staging/ev/", cols = {"dt"}, paths = staging},
    {base = "", cols = {"dt"}, paths = {
        "_tmp/dt=2024-01-01/part-1.parquet",
        "dt=2024-01-01/_SUCCESS",
        "dt=2024-01-01/part-0.parquet",
    }},
    {base = "_staging/raw", cols = {}, paths = {
        "_staging/raw",
        "_staging/raw/_SUCCESS",
        "_staging/raw/f1.csv",
        "_staging/raw/f2.csv",
    }},
    -- partition value starting with "_" is not hidden
    {base = "_letters", cols = {"a"}, paths = {
        "_letters/a=_foo/x.csv",
    }},
    -- partition column name starting with "_" stays hidden (as in Spark)
    {base = "_letters", cols = {"_a"}, paths = {
        "_letters/_a=1/x.csv",
    }},
    {base = "_staging/empty", cols = {"dt"}, paths = {
        "_staging/empty/_SUCCESS",
    }},
}

for _, c in ipairs(cases) do
    print("# base: '" .. c.base .. "' columns: [" .. table.concat(c.cols, ",") .. "]")
    local pager = hive.extract_partition_pager(mock_client(c.paths), "repo", "commit", c.base, c.cols, 2)
    for part_key, entries in pager do
        print("partition: '" .. part_key .. "'")
        for _, entry in ipairs(entries) do
            print("path: " .. entry.path)
        end
    end
end
print("# done")
