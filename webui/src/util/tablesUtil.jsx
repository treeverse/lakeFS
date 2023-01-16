import React from "react";
import {objects} from "../lib/api";
import {useAPI} from "../lib/hooks/api";
import {TableType, TreeItemType} from "../constants";

function isDeltaLogPrefix(obj) {
    return obj.path.includes("_delta_log/");
}

/**
 * Checks whether a path is a table root and calculates its type.
 *
 * @param path the table path
 * @param repo the repo for the path
 * @param refs the ref in which the path is expected to be listed.
 * @return the type of table if the path is a table root, and null otherwise (for paths to objects or prefixes).
 */
export function getTableType(path, repo, ref) {
    let listedItems = null;
    const listResponse = useAPI(() => objects.list(repo.id, ref, path), []);
    if (listResponse.response !== null && listResponse.response.results.length !== 0) {
            listedItems = listResponse.response.results;
    }

    if (listedItems !== null) {
        for (const item of listedItems) {
            if (isDeltaLogPrefix(item)) {
                return TableType.DeltaLake;
            }
        }
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
