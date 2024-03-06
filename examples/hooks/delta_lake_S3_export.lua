--[[
    args:
        - table_defs (e.g. ["table1.yaml", "table2", ...])
        - lakefs.access_key_id
        - lakefs.secret_access_key
        - aws.access_key_id
        - aws.secret_access_key
        - aws.region
        - region
]]
local aws = require("aws")
local formats = require("formats")
local delta_export = require("lakefs/catalogexport/delta_exporter")
local json = require("encoding/json")

local table_descriptors_path = "_lakefs_tables"
local sc = aws.s3_client(args.aws.access_key_id, args.aws.secret_access_key, args.aws.region)

local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key, args.aws.region)
local delta_table_details = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, table_descriptors_path)
for t, details in pairs(delta_table_details) do
    print("Delta Lake exported table \"" .. t .. "\"'s location: " .. details["path"] .. "\n")
    print("Delta Lake exported table \"" .. t .. "\"'s metadata:\n")
    for k, v in pairs(details["metadata"]) do
        if type(v) == "table" then
            print("\t" .. k .. " = " .. json.marshal(v) .. "\n")
        else
            print("\t" .. k .. " = " .. v .. "\n")
        end
    end
end
