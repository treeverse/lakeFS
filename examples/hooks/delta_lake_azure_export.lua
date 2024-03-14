--[[
    args:
        - table_defs (e.g. ["table1.yaml", "table2", ...])
        - lakefs.access_key_id
        - lakefs.secret_access_key
        - azure.storage_account
        - azure.access_key
]]
local azure = require("azure")
local formats = require("formats")
local delta_exporter = require("lakefs/catalogexport/delta_exporter")

local table_descriptors_path = "_lakefs_tables"
local sc = azure.blob_client(args.azure.storage_account, args.azure.access_key)
local function write_object(_, key, buf)
    return sc.put_object(key,buf)
end
local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key)
local delta_table_details = delta_exporter.export_delta_log(action, args.table_defs, write_object, delta_client, table_descriptors_path)

for t, details in pairs(delta_table_details) do
    print("Delta Lake exported table \"" .. t .. "\"'s location: " .. details["path"] .. "\n")
end
