import {branches, commits, metaRanges, NotFoundError, ranges} from "../../../lib/api";

const runImport = async (updateImportState, isSourceValid, prependPath, commitMsg, sourceRef, branch, repoId, refId) => {
    let paginationResp = {};
    let after = "";
    let importBranchResp;
    let sum = 0;
    const rangeArr = [];
    updateImportState(true, null, false, sum);
    do {
        const response = await ranges.createRange(repoId, sourceRef, after, prependPath, paginationResp.continuation_token);
        rangeArr.push(response.range);
        paginationResp = response.pagination;
        after = paginationResp.last_key;
        sum += response.range.count;
        updateImportState(true, null, false, sum);
    } while (paginationResp.has_more);
    const metarange = await metaRanges.createMetaRange(repoId, rangeArr);

    try {
        importBranchResp = await branches.get(repoId, branch);
    } catch (error) {
        if (error instanceof NotFoundError) {
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
            importBranchResp = await branches.get(repoId, branch);
        } else {
            throw error;
        }
    }

    await commits.commit(repoId, importBranchResp.id, commitMsg, {}, metarange.id);
    updateImportState(false, null, true, sum);
}

export default runImport