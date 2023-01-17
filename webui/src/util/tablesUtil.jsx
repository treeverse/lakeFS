import React from "react";
import {objects} from "../lib/api";
import {useAPI} from "../lib/hooks/api";
import {TableType, TreeItemType} from "../constants";

/**
 * Checks whether a path is a table root and calculates its type.
 *
 * @param path the table path
 * @param repo the repo for the path
 * @param refs the ref in which the path is expected to be listed.
 * @return the type of table if the path is a table root, and null otherwise (for paths to objects or prefixes).
 */
export function getTableType(path, repo, ref) {
    const listResponse = useAPI(() => objects.list(repo.id, ref, path + "_delta_log/"), []);
    if (listResponse.response !== null && listResponse.response.results.length !== 0) {
        return TableType.DeltaLake;
    }
    return null;
}

export function tableTypetoTreeItemType(tableType) {
    switch (tableType) {
        case TableType.DeltaLake:
            return TreeItemType.DeltaLakeTable;
        default:
            throw Error("unsupported table type");
    }
}
