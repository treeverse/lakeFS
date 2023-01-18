import React from "react";
import {objects} from "../lib/api";
import {useAPI} from "../lib/hooks/api";

/**
 * Checks whether a path is a delta table root.
 *
 * @param path the table path
 * @param repo the repo for the path
 * @param ref the ref in which the path is expected to be listed.
 * @return true if the path is a delat table root, false otherwise.
 */
export function isDeltaLakeTable(path, repo, ref) {
    const listResponse = useAPI(() => objects.list(repo.id, ref, path + "_delta_log/"), []);
    if (listResponse.response !== null && listResponse.response.results.length !== 0) {
        return true;
    }
    return false;
}
