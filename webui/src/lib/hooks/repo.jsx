import React, {useContext, useState, createContext, useEffect} from "react";

import {repositories, branches, commits, NotFoundError, tags} from "../api";
import {useRouter} from "./router";


export const resolveRef = async (repoId, refId) => {
    // try branch
    try {
        const branch = await branches.get(repoId, refId);
        return {id: branch.id, type: 'branch'};
    } catch(error) {
        if (!(error instanceof NotFoundError)) {
            throw error;
        }
    }
    // try tag
    try {
        const tag = await tags.get(repoId, refId);
        return {id: tag.id, type: 'tag'};
    } catch(error) {
        if (!(error instanceof NotFoundError)) {
            throw error;
        }
    }
    // try commit
    try {
        const commit = await commits.get(repoId, refId);
        return {id: commit.id,  type: 'commit'};
    } catch(error) {
        if (!(error instanceof NotFoundError)) {
            throw error;
        }
    }

    throw new NotFoundError('ref not found');
};


const RefContext =  createContext(null);

export const useRefs = () => {
    const [ refs ] = useContext(RefContext);
    return refs;
}

export const useRefsWithRefresh = () => {
    return useContext(RefContext);
};

const refContextInitialState = {
    loading: true,
    error: null,
    repo: null,
    reference: null,
    compare: null
};

export const RefContextProvider = ({ children }) => {
    const router = useRouter();
    const { repoId } = router.params;
    const {ref, compare} = router.query;

    const [refState, setRefState] = useState(refContextInitialState);

    useEffect(() => {
        const fetch = async () => {
            setRefState(refContextInitialState);
            if (!repoId) return;
            try {
                const repo = await repositories.get(repoId);
                const reference = await resolveRef(repoId, (!!ref) ? ref : repo.default_branch);
                const comparedRef = await resolveRef(repoId, (!!compare)? compare : repo.default_branch);
                setRefState({...refContextInitialState, loading: false, repo, reference, compare: comparedRef});
            } catch (err) {
                setRefState({...refContextInitialState, loading: false, error: err});
            }
        };
        fetch();
    }, [repoId, ref, compare]);

    return (
        <RefContext.Provider value={[refState, fetch]}>
            {children}
        </RefContext.Provider>
    );
};