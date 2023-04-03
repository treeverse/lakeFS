import {objects, otfDiffs} from "../lib/api";

/**
 * Checks whether a path is a delta table root.
 *
 * @param entry the table entry
 * @param repo the repo for the path
 * @param ref the ref in which the path is expected to be listed.
 * @return true if the path is a delat table root, false otherwise.
 */
export async function isDeltaLakeTable(entry, repo, ref) {
    if (entry.path_type === "object") {
        return
    }

    let response = await objects.list(repo.id, ref, entry.path + "_delta_log/")
    return response !== null && response.results.length !== 0;
}

export async function isDeltaLakeDiffEnabled() {
    let enabledDiffs = await otfDiffs.get();
    if (enabledDiffs === null || enabledDiffs.diffs.length === 0) {
        return false
    }
    return enabledDiffs.diffs.includes("delta");
}
