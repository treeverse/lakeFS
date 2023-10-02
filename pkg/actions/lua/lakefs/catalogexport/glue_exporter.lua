local pathlib = require("path")
local json = require("encoding/json")
local lakefs = require("lakefs")
local extractor = require("lakefs/catalogexport/table_extractor")
local utils = require("lakefs/catalogexport/internal")
local sym_exporter = require("lakefs/catalogexport/symlink_exporter")

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

local function hive_col_to_glue(col)
    return {
        Name = col.name,
        Type = typesMapping[col.type] or col.type,
        Comment = col.comment,
        Parameters = col.parameters
    }
end

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
    local export_base_uri = sym_exporter.get_storage_uri_prefix(storage_base_prefix, commit_id, action_info)
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

local function build_glue_create_table_input_from_hive(descriptor, base_input, action_info, options)
    local opts = options or {}
    -- get table symlink location uri 
    local base_prefix = opts.export_base_uri or action_info.storage_namespace
    local symlink_location = get_table_location(base_prefix, descriptor, action_info)

    -- convert hive cols/partitions to glue
    local partitions = hive_partitions_to_glue_input(descriptor)
    local cols = hive_columns_to_glue_input(descriptor)
    return build_glue_create_table_input(base_input, descriptor, symlink_location, cols, partitions, action_info, opts)
end

-- create table in glue
local function export_glue(glue, db, table_src_path, create_table_table_input, action_info, options)
    local opts = options or {}
    local repo_id = action_info.repository_id
    local commit_id = action_info.commit_id
    -- get table desctiptor
    local descriptor = extractor.get_table_descriptor(lakefs, repo_id, commit_id, table_src_path)

    -- build table creation input for glue
    local table_input = opts.override_create_table_input or
                            build_glue_create_table_input_from_hive(descriptor, create_table_table_input, action_info,
            opts)
    -- create table
    local json_input = json.marshal(table_input)
    if opts.debug then
        print("Creating Glue Table - input:", json_input)
    end
    glue.create_table(db, json_input)
    return {
        create_table_input = table_input
    }
end

return {
    build_glue_create_table_input_from_hive=build_glue_create_table_input_from_hive,
    export_glue = export_glue
}
