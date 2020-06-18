import React, {useState, useRef, useEffect, useCallback} from 'react';

import Popover from "react-bootstrap/Popover";
import Overlay from "react-bootstrap/Overlay";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Badge from "react-bootstrap/Badge";
import {ChevronDownIcon, ChevronUpIcon, ChevronRightIcon, XIcon} from "@primer/octicons-react";

import * as api from "../actions/api";


const BranchSelector = ({ repo, selected, branches, filterBranches, listBranches, selectRef, withCommits, withWorkspace, amount = 1000 }) => {

    // used for branch pagination
    const [from, setFrom] = useState("");

    // used for commit listing
    const initialCommitList = {branch: selected, commits: null, loading: false};
    const [commitList, setCommitList] = useState(initialCommitList);

    useEffect(()=> {
        listBranches(repo.id, from, amount);
    }, [listBranches, repo.id, amount, from]);

    const form = (
        <div className="ref-filter-form">
            <Form onSubmit={e => { e.preventDefault(); }}>
                <Form.Control type="text" placeholder="filter branches" onChange={(e)=> {
                    filterBranches(repo.id, e.currentTarget.value, amount);
                }}/>
            </Form>
        </div>
    );

    if (branches.loading) {
        return  (
            <div className="ref-selector">
                {form}
                <p>Loading...</p>
            </div>
        );
    }

    if (!!branches.error) {
        return  (
            <div className="ref-selector">
                {form}
                <Alert variant="danger">{branches.error}</Alert>
            </div>
        );
    }

    if (commitList.commits !== null) {
        return (
            <CommitList
                withWorkspace={withWorkspace}
                commits={commitList.commits}
                branch={commitList.branch}
                selectRef={selectRef}
                reset={() => {
                    setCommitList(initialCommitList);
                }}/>
        );
    }


    const results = branches.payload.results;

    return (
        <div className="ref-selector">
            {form}
            <div className="ref-scroller">
                <ul className="list-group ref-list">
                    {results.map(branch => (
                        <BranchEntry key={branch.id} repo={repo} branch={branch} selectRef={selectRef} selected={selected} withCommits={withCommits} logCommits={async () => {
                            const data = await api.commits.log(repo.id, branch.id, '', 0);
                            setCommitList({...commitList, branch: branch, commits: data.results});
                        }}/>
                    ))}
                </ul>
                <Paginator results={branches.payload.results} pagination={branches.payload.pagination} from={from} onPaginate={setFrom}/>
            </div>
        </div>
    );
};

const CommitList = ({ commits, selectRef, reset, branch, withWorkspace }) => {
    const getMessage = commit => {
        if (!commit.message) {
            return 'repository epoch';
        }

        if (commit.message.length > 60) {
            return commit.message.substr(0, 40) + '...';
        }

        return commit.message;
    };

    return (
        <div className="ref-selector">
            <h5>{branch.id}</h5>
            <div className="ref-scroller">
                <ul className="list-group ref-list">
                    {(withWorkspace) ? (
                        <li className="list-group-item" key={branch.id}>
                            <Button variant="link" onClick={() => {
                                selectRef({id: branch.id, type: 'branch'});
                            }}><em>{branch.id}'s Workspace (uncommitted changes)</em></Button>
                        </li>
                    ) : (<span/>)}
                    {commits.map(commit => (
                        <li className="list-group-item" key={commit.id}>
                                <Button variant="link" onClick={() => {
                                    selectRef({id: commit.id, type: 'commit'});
                                }}>{getMessage(commit)} </Button>
                            <div className="actions">
                                <Badge variant="light">{commit.id.substr(0, 16)}</Badge>
                            </div>
                        </li>
                    ))}
                </ul>
                <p className="ref-paginator">
                    <Button variant="link" size="sm" onClick={reset}>Back</Button>
                </p>
            </div>
        </div>
    );
};

const BranchEntry = ({repo, branch, selectRef, selected, logCommits, withCommits}) => {
    return (
        <li className="list-group-item" key={branch.id}>
            {(!!selected && selected.type === 'branch' && branch.id === selected.id) ?
                <strong>{branch.id}</strong> :
                <Button variant="link" onClick={() => {
                    selectRef({id: branch.id, type: 'branch'});
                }}>{branch.id}</Button>
            }
            <div className="actions">
                {(branch.id === repo.default_branch) ? (<Badge variant="info">Default</Badge>) : <span/>}
                {(withCommits) ? (
                    <Button onClick={logCommits} size="sm" variant="link">
                        <ChevronRightIcon/>
                    </Button>
                ) : (<span/>)}
            </div>
        </li>
    );
};

const Paginator = ({ pagination, onPaginate, results, from }) => {
    const next = (!!results.length) ? results[results.length-1].id : "";

    if (!pagination.has_more && from === "") return (<span/>);

    return (
        <p className="ref-paginator">
            {(from !== "") ?
                (<Button  size={"sm"} variant="link" onClick={() => { onPaginate(""); }}>Reset</Button>) :
                (<span/>)
            }
            {' '}
            {(pagination.has_more) ?
                (<Button size={"sm"} variant="link" onClick={() => { onPaginate(next); }}>Next...</Button>) :
                (<span/>)
            }
        </p>
    );
};


const RefDropdown = ({ repo, selected, selectRef, onCancel, prefix = '', emptyText = '', withCommits = true, withWorkspace = true }) => {
    const [show, setShow] = useState(false);
    const target = useRef(null);

    const [branches, setBranches] = useState({loading: true, payload: null, error: null});

    const listBranches = useCallback(async (repoId, from, amount) => {
        setBranches({loading: true, payload: null, error: null});
        try {
            const response = await api.branches.list(repoId, from, amount);
            setBranches({loading: false, payload: response, error: null});
        } catch (error) {
            setBranches({loading: false, payload: null, error: error});
        }
    }, [] );

    const filterBranches = useCallback(async (repoId, from, amount) => {
        setBranches({loading: true, payload: null, error: null});
        try {
            const response = await api.branches.filter(repoId, from, amount);
            setBranches({loading: false, payload: response, error: null});
        } catch (error) {
            setBranches({loading: false, payload: null, error: error});
        }
    }, []);

    const popover = (
        <Overlay target={target.current} show={show} placement="bottom">
            <Popover className="ref-popover">
                <Popover.Content>
                    <BranchSelector
                        repo={repo}
                        branches={branches}
                        withCommits={withCommits}
                        listBranches={listBranches}
                        filterBranches={filterBranches}
                        withWorkspace={withWorkspace}
                        selected={selected}
                        selectRef={(ref) => {
                            selectRef(ref);
                            setShow(false);
                        }}/>
                </Popover.Content>
            </Popover>
        </Overlay>
    );
    
    const cancelButton = (!!onCancel && !!selected) ? (<Button onClick={() => {
        setShow(false);
        onCancel();
    }} variant="light"><XIcon/></Button>) : (<span/>);

    if (!selected) {
        return (
            <>
                <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                    {emptyText} {show ? ChevronUpIcon : ChevronDownIcon}
                </Button>
                {cancelButton}
                {popover}
            </>
        );
    }

    const title = prefix + ((selected.type === 'branch') ? 'Branch: ' : 'Commit: ');
    const selectedIdDisplay = (selected.type === 'branch') ? selected.id : selected.id.slice(0, 16);

    return (
        <>
            <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                {title} <strong>{selectedIdDisplay}</strong> {show ? ChevronUpIcon : ChevronDownIcon}
            </Button>
            {cancelButton}
            {popover}
        </>
    );
};

export default RefDropdown;
