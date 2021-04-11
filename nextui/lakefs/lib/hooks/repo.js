import {useContext, useState, createContext, useEffect} from "react";
import {repositories, branches, commits, NotFoundError} from "../../rest/api";
import {useAPI} from "../../rest/hooks";
import {useRouter} from "next/router";

const RefContext =  createContext(null);

export const useRefs = () => {
    return useContext(RefContext)
}

export const RefContextProvider = ({ children }) => {

    const router = useRouter()

    const { repoId, ref, compare } = router.query

    const initialState = {
        loading: true,
        error: null,
        repo: null,
        reference: null,
        compare: null
    }
    const [refState, setRefState] = useState(initialState)

    const fetch = async () => {
        setRefState(initialState)
        if (!repoId) return
        try {
            const repo = await repositories.get(repoId)
            const reference = await resolveRef(repoId, (!!ref) ? ref: repo.default_branch)
            let comparedRef = null;
            if (!!compare) comparedRef = await resolveRef(repoId, compare)
            setRefState({...initialState, loading: false, repo, reference, compare: comparedRef})
        } catch (err) {
            setRefState({...initialState, loading: false, error: err})
        }
    }

    useEffect(fetch, [repoId, ref, compare])

    return (
        <RefContext.Provider value={refState}>
            {children}
        </RefContext.Provider>
    )
}


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

    throw new NotFoundError('ref not found');
}

export const useRepoAndRef = (repoId, refId) => {
    return useAPI(async () => {
        const repo = await repositories.get(repoId)
        try {
            const ref = await resolveRef(repoId, (!!refId) ? refId : repo.default_branch)
            return {repo, ref}
        } catch (error) {
            throw error
        }

    }, [repoId, refId]);
}

export const useRepoRefAndCompare = (repoId, refId, compareId) => {
    return useAPI(async () => {
        const repo = await repositories.get(repoId)
        const ref = await resolveRef(repoId, (!!refId) ? refId : repo.default_branch)
        let compare = null
        if (!!compareId)
            compare = await resolveRef(repoId, compareId)
        return {repo, ref, compare}
    }, [repoId, refId, compareId])
}

export const useRepo = (repoId) => {
    return useAPI(() => {
        return repositories.get(repoId)
    }, [repoId])
}
