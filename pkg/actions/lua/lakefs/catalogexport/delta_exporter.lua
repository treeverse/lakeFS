local lakefs = require("lakefs")
local formats = require("formats")
local pathlib = require("path")
local json = require("encoding/json")
local utils = require("lakefs/catalogexport/internal")

--[[
    delta_log_entry_key_generator returns a closure that returns a Delta Lake version key according to the Delta Lake
    protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries
    Example:
        local gen = delta_log_entry_key_generator()
        gen() -- 000000000000000000.json
        gen() -- 000000000000000001.json
]]
local function delta_log_entry_key_generator()
    local current = 0
    return function()
        local delta_log_entry_length = 20
        local key = tostring(current)
        local padding_length = delta_log_entry_length - key:len()
        local padded_key = ""
        for _ = 1, padding_length do
            padded_key = padded_key .. "0"
        end
        padded_key = padded_key .. key .. ".json"
        current = current + 1
        return padded_key
    end
end

--[[
    action:
        - repository_id
        - commit_id

   table_paths: ["path/to/table1", "path/to/table2", ...]

    write_object: function(bucket, key, data)

    delta_client:
        - get_table: function(repo, ref, prefix)

]]
local function export_delta_log(action, table_paths, write_object, delta_client)
    local repo = action.repository_id
    local commit_id = action.commit_id

    local ns = action.storage_namespace
    if ns == nil then
        error("failed getting storage namespace for repo " .. repo)
    end
    local response = {}
    for _, path in ipairs(table_paths) do
        local t = delta_client.get_table(repo, commit_id, path)
        local sortedKeys = utils.sortedKeys(t)
        --[[ Pairs of (version, map of json content):
                (1,
                {
                    {"commitInfo":{"timestamp":1699276565259,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":9,"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numFiles":"1","numOutputRows":"4","numOutputBytes":"1353"},"engineInfo":"Apache-Spark/3.3.2 Delta-Lake/2.3.0","txnId":"eb6816ae-404f-4338-9e1a-2cb0a4626ab3"}}
                    {"add":{"path":"part-00000-a5a20e52-2b3d-440b-97a8-829fbc4a2804-c000.snappy.parquet","partitionValues":{},"size":1353,"modificationTime":1699276565000,"dataChange":true,"stats":"{\"numRecords\":4,\"minValues\":{\"Hylak_id\":18,\"Lake_name\":\" \",\"Country\":\"Malawi\",\"Depth_m\":3.0},\"maxValues\":{\"Hylak_id\":16138,\"Lake_name\":\"Malombe\",\"Country\":\"Malawi\",\"Depth_m\":706.0},\"nullCount\":{\"Hylak_id\":0,\"Lake_name\":0,\"Country\":0,\"Depth_m\":0}}"}}
                    {"remove":{"path":"part-00000-d660b401-ceec-415a-a791-e8d1c7599e3d-c000.snappy.parquet","deletionTimestamp":1699276565259,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":82491}}
                }
                )
        ]]
        local table_log = {}
        local keyGenerator = delta_log_entry_key_generator()
        for _, key in ipairs(sortedKeys) do
            local content = t[key]
            local entry_log = {}
            --[[
                An array of entries:
                    {"commitInfo":{"timestamp":1699276565259,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":9,"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numFiles":"1","numOutputRows":"4","numOutputBytes":"1353"},"engineInfo":"Apache-Spark/3.3.2 Delta-Lake/2.3.0","txnId":"eb6816ae-404f-4338-9e1a-2cb0a4626ab3"}},
                    {"add":{"path":"part-00000-a5a20e52-2b3d-440b-97a8-829fbc4a2804-c000.snappy.parquet","partitionValues":{},"size":1353,"modificationTime":1699276565000,"dataChange":true,"stats":"{\"numRecords\":4,\"minValues\":{\"Hylak_id\":18,\"Lake_name\":\" \",\"Country\":\"Malawi\",\"Depth_m\":3.0},\"maxValues\":{\"Hylak_id\":16138,\"Lake_name\":\"Malombe\",\"Country\":\"Malawi\",\"Depth_m\":706.0},\"nullCount\":{\"Hylak_id\":0,\"Lake_name\":0,\"Country\":0,\"Depth_m\":0}}"}},
                    {"remove":{"path":"part-00000-d660b401-ceec-415a-a791-e8d1c7599e3d-c000.snappy.parquet","deletionTimestamp":1699276565259,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":82491}}
            ]]
            for _, e in ipairs(content) do
                local entry = json.unmarshal(e)
                local p = ""
                if entry.add ~= nil then
                    p = entry.add.path
                elseif entry.remove ~= nil then
                    p = entry.remove.path
                end
                if p ~= "" then
                    local code, obj = lakefs.stat_object(repo, commit_id, pathlib.join("/",path, p))
                    if code == 200 then
                        local obj_stat = json.unmarshal(obj)
                        local physical_path = obj_stat["physical_address"]
                        if entry.add ~= nil then
                            entry.add.path = physical_path
                        elseif entry.remove ~= nil then
                            entry.remove.path = physical_path
                        end
                    end
                end
                local entry_m = json.marshal(entry)
                table.insert(entry_log, entry_m)
            end
            table_log[keyGenerator()] = entry_log
        end

        -- Get the table delta log physical location
        local t_name = pathlib.parse(path)["base_name"]
        local table_export_prefix = utils.get_storage_uri_prefix(ns, commit_id, action)
        local table_physical_path = pathlib.join("/", table_export_prefix, t_name)
        local table_log_physical_path = pathlib.join("/", table_physical_path, "_delta_log")

        -- Upload the log to this physical_address
        local storage_props = utils.parse_storage_uri(table_log_physical_path)
        --[[
            table_log:
                {
                    <version1>.json : [
                        {"commitInfo":...}, {"add": ...}, {"remove": ...},...
                    ],
                    <version2>.json : [
                        {"commitInfo":...}, {"add": ...}, {"remove": ...},...
                    ],...
                }
        ]]
        for entry_version, table_entry in pairs(table_log) do
            local table_entry_string = ""
            for _, content_entry in ipairs(table_entry) do
                table_entry_string = table_entry_string .. content_entry
            end
            local version_key = storage_props.key .. "/" .. entry_version
            write_object(storage_props.bucket, version_key, table_entry_string)
        end
        response[t_name] = table_physical_path
    end
    return response
end

return {
    export_delta_log = export_delta_log,
}
