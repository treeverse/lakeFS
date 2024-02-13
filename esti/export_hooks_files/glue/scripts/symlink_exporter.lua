
local exporter = require("lakefs/catalogexport/symlink_exporter")
local aws = require("aws")
local table_path = args.table_source
local s3 = aws.s3_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
exporter.export_s3(s3, table_path, action, {debug=true})