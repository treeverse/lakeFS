import useSWR from "swr";

import {repositories, branches, commits, NotFoundError} from "../../rest/api";


const resolve = async (repoId, refId) => {
    // try branch
    try {
        const branch = await branches.get(repoId, refId)
        return {id: branch.id, type: 'branch'}
    } catch(error) {
        if (!(error instanceof NotFoundError)) {
            throw error
        }
    }
    // try commit
    try {
        const commit = await commits.get(repoId, refId)
        return {id: commit.id,  type: 'commit'}
    } catch(error) {
        if (!(error instanceof NotFoundError)) {
            throw error
        }
    }

    throw NotFoundError('ref not found');
}

export const useRepoAndRef = (repoId, refId) => {
    return useSWR(`/repoAndRef/${repoId}/${refId}`, async () => {
        try {
            const repo = await repositories.get(repoId);
            const ref = await resolve(repoId, (!!refId) ? refId : repo.default_branch);
            return {repo, ref}
        } catch (error) {
            return {error}
        }
    });
}
