--[[
    action_args:
        - repo
        - commit_id

    export_delta_args:
        - table_paths: ["path/to/table1", "path/to/table2", ...]
        - lakefs_key
        - lakefs_secret
        - region

    storage_client:
        - put_object: function(bucket, key, data)
]]

local export_delta_args = {
    table_paths = args.table_paths,
    lakefs_key = args.lakefs.access_key_id,
    lakefs_secret = args.lakefs.secret_access_key,
    region = args.aws.aws_region
}

local aws = require("aws")
local delta = require("lakefs/catalogexport/delta_exporter")
local sc = aws.s3_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)

local delta_table_locations = delta.export_delta_log(action, export_delta_args, sc)
for t, loc in pairs(delta_table_locations) do
    print("Delta Lake exported table \"" .. t .. "\"'s location: " .. loc .. "\n")
end
