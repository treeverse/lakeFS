import React from "react";

import {objects} from "../lib/api";

/**
 * Checks whether a path is a delta table root.
 *
 * @param path the table path
 * @param repo the repo for the path
 * @param ref the ref in which the path is expected to be listed.
 * @return true if the path is a delat table root, false otherwise.
 */
export async function isDeltaLakeTable(path, repo, ref) {
    let response = await objects.list(repo.id, ref, path + "_delta_log/")
    if (response !== null && response.results.length !== 0) {
        return true;
    }
    return false;
}


