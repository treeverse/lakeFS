
import {objects} from "../lib/api";
import {useAPI} from "../lib/hooks/api";

/**
 * Checks whether a path is a delta table root.
 *
 * @param entry the table entry
 * @param repo the repo for the path
 * @param ref the ref in which the path is expected to be listed.
 * @return true if the path is a delat table root, false otherwise.
 */
// export async function isDeltaLakeTable(entry, repo, ref) {
//     if (entry.path_type === "object") {
//         return
//     }
//
//     let response = await objects.list(repo.id, ref, entry.path + "_delta_log/")
//     if (response !== null && response.results.length !== 0) {
//         return true;
//     }
//     return false;
// }

export function isDeltaLakeTable(path, repo, ref) {
    const listResponse = useAPI(() => objects.list(repo.id, ref, path + "_delta_log/"), []);
    if (listResponse.response !== null && listResponse.response.results.length !== 0) {
        return true;
    }
    return false;
}


