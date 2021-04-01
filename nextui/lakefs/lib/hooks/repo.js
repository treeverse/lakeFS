import {repositories, branches, commits, NotFoundError} from "../../rest/api";
import {useAPI} from "../../rest/hooks";


export const resolveRef = async (repoId, refId) => {
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
    return useAPI(async () => {
        const repo = await repositories.get(repoId);
        const ref = await resolveRef(repoId, (!!refId) ? refId : repo.default_branch);
        return {repo, ref}
    }, [repoId, refId]);
}

export const useRepo = (repoId) => {
    return useAPI(() => {
        return repositories.get(repoId)
    }, [repoId])
}
