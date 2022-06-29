import {branches, commits, metaRanges, NotFoundError, ranges} from "../../../lib/api";

const runImport = async (updateImportState, prependPath, commitMsg, sourceRef, branch, repoId, refId) => {
    let paginationResp = {};
    let after = "";
    let importBranchResp;
    let sum = 0;
    const rangeArr = [];
    const importStatusUpdate = {
        inProgress: true,
        done: false,
        numObj: sum,
    }
    updateImportState(importStatusUpdate);
    do {
        const response = await ranges.createRange(repoId, sourceRef, after, prependPath, paginationResp.continuation_token);
        rangeArr.push(response.range);
        paginationResp = response.pagination;
        after = paginationResp.last_key;
        sum += response.range.count;
        importStatusUpdate.numObj = sum
        updateImportState(importStatusUpdate);
    } while (paginationResp.has_more);
    const metarange = await metaRanges.createMetaRange(repoId, rangeArr);

    try {
        importBranchResp = await branches.get(repoId, branch);
    } catch (error) {
        if (error instanceof NotFoundError) {
            importBranchResp = await createBranch(repoId, refId, branch);
        } else {
            throw error;
        }
    }
    await commits.commit(repoId, importBranchResp.id, commitMsg, {}, metarange.id);
    importStatusUpdate.done = true;
    importStatusUpdate.inProgress = false;
    updateImportState(importStatusUpdate);
}

const createBranch = async (repoId, refId, branch) => {
    // Find root commit for repository
    let hasMore = true;
    let nextOffset = "";
    let baseCommit = refId;
    do {
        let response = await commits.log(repoId, refId, nextOffset, 1000);
        hasMore = response.pagination.has_more;
        nextOffset = response.pagination.next_offset;
        baseCommit = response.results.at(-1);
    } while (hasMore)
    await branches.create(repoId, branch, baseCommit.id);
    return await branches.get(repoId, branch);
}

export default runImport