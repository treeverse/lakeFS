local pathlib = require("path")
local json = require("encoding/json")
local lakefs = require("lakefs")
local extractor = require("lakefs/catalogexport/table_extractor")
local utils = require("lakefs/catalogexport/internal")
local sym_exporter = require("lakefs/catalogexport/symlink_exporter")

--[[
    Generate glue table name
    @descriptor(Table): object from (i.e _lakefs_tables/my_table.yaml)
    @action_info(Table): the global action object
]]
local function get_full_table_name(descriptor, action_info)
    local commit_id = action_info.commit_id
    local repo_id = action_info.repository_id
    local branch_or_tag = utils.ref_from_branch_or_tag(action_info)
    local sha = utils.short_digest(commit_id)
    return string.format("%s_%s_%s_%s", descriptor.name, repo_id, branch_or_tag, sha)
end

-- map hive to glue types
local typesMapping = {
    integer = "int"
}

-- helper function to convert hive col to part of glue create table input
local function hive_col_to_glue(col)
    return {
        Name = col.name,
        Type = typesMapping[col.type] or col.type,
        Comment = col.comment,
        Parameters = col.parameters
    }
end

-- Create list of partitions for Glue input from a Hive descriptor
local function hive_partitions_to_glue_input(descriptor)
    local partitions = {}
    local cols = descriptor.schema.fields or {}
    -- columns list to map by name
    for _, c in ipairs(cols) do
        cols[c.name] = c
    end
    -- iterate partitions order and find them in the fields, the order determines the path in storage
    for _, part_key in ipairs(descriptor.partition_columns) do
        local col = cols[part_key]
        if col == nil then
            error(string.format("partition name `%s` not found in table `%s`", part_key, descriptor.name))
        end
        table.insert(partitions, hive_col_to_glue(col))
    end
    return partitions
end

-- Create list of columns for Glue excluding partitions
local function hive_columns_to_glue_input(descriptor)
    -- create set of partition names since they must not appear in the columns input in glue
    local partition_names = {}
    for _, p in ipairs(descriptor.partition_columns) do
        partition_names[p] = true
    end
    -- create columns as inputs for glue
    local columns = {}
    local cols = descriptor.schema.fields or {}
    for _, col in ipairs(cols) do
        if not partition_names[col.name] then -- not a partition
            table.insert(columns, hive_col_to_glue(col))
        end
    end
    return columns
end

-- default location value (e.g root location of either partitions or flat symlink.txt file)
local function get_table_location(storage_base_prefix, descriptor, action_info)
    local commit_id = action_info.commit_id
    local export_base_uri = utils.get_storage_uri_prefix(storage_base_prefix, commit_id, action_info)
    return pathlib.join("/", export_base_uri, descriptor.name)
end

-- create a standard AWS Glue table input (i.e not Apache Iceberg), add input values to base input and configure the rest
local function build_glue_create_table_input(base_input, descriptor, symlink_location, columns, partitions, action_info,
    options)
    local input = utils.deepcopy(base_input)
    local opts = options or {}
    input.Name = opts.table_name or get_full_table_name(descriptor, action_info)
    input.PartitionKeys = array(partitions)
    input.TableType = "EXTERNAL_TABLE"
    input.StorageDescriptor.Columns = array(columns)
    input.StorageDescriptor.Location = symlink_location
    return input
end

--[[
    create a standard glue table in glue catalog
    @glue: AWS glue client
    @db(string): glue database name
    @table_src_path(string): path to table spec (i.e _lakefs_tables/my_table.yaml)
    @create_table_input(Table): struct mapping to table_input in AWS https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html#API_CreateTable_RequestSyntax
    should contain inputs describing the data format (i.e InputFormat, OutputFormat, SerdeInfo) since the exporter is agnostic to this.
    by default this function will configure table location and schema.
    @action_info(Table): the global action object
    @options:
    - table_name(string): override default glue table name
    - debug(boolean)
    - export_base_uri(string): override the default prefix in S3 for symlink location i.e s3://other-bucket/path/
    - create_db_input(table): parameters for creating the database. If nil, then the DB must already exisst
]]
local function export_glue(glue, db, table_src_path, create_table_input, action_info, options)
    local opts = options or {}
    local repo_id = action_info.repository_id
    local commit_id = action_info.commit_id

    -- get table desctiptor from _lakefs_tables/
    local descriptor = extractor.get_table_descriptor(lakefs, repo_id, commit_id, table_src_path)

    -- get table symlink location uri
    local base_prefix = opts.export_base_uri or action_info.storage_namespace
    local symlink_location = get_table_location(base_prefix, descriptor, action_info)

    -- parse Hive table
    local columns = {}
    local partitions = {}
    if descriptor.type == "hive" then
        -- convert hive cols/partitions to glue
        partitions = hive_partitions_to_glue_input(descriptor)
        columns = hive_columns_to_glue_input(descriptor)
    else
        error("table " .. descriptor.type .. " in path " .. table_src_path .. " not supported")
    end

    if opts.create_db_input ~= nil then
        local dbopts = { error_on_already_exists = false, create_db_input = opts.create_db_input }
        glue.create_database(db, dbopts)
        if opts.debug then
            print("success creating / verifying glue database")
        end
    end

    -- finallize create glue table input
    local table_input = build_glue_create_table_input(create_table_input, descriptor, symlink_location, columns,
        partitions, action_info, opts)

    -- create table
    local json_input = json.marshal(table_input)
    if opts.debug then
        print("Creating Glue Table - input:", json_input)
    end
    glue.create_table(db, json_input)
    return {
        table_input = table_input
    }
end

return {
    get_full_table_name = get_full_table_name,
    export_glue = export_glue
}
