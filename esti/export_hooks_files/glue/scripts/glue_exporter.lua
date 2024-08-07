local aws = require("aws")
local exporter = require("lakefs/catalogexport/glue_exporter")
action.commit_id = "{{ .OverrideCommitID }}" -- override commit id to use specific symlink file previously created
local glue = aws.glue_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
local create_db_input_table = {DatabaseInput = {Description="Created by Glue Exporter"}}
exporter.export_glue(glue, args.catalog.db_name, args.table_source, args.catalog.table_input, action, {debug=true, create_db_input = create_db_input_table})
