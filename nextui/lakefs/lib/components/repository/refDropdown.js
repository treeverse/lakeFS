import {useCallback, useEffect, useRef, useState} from "react";

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import Overlay from "react-bootstrap/Overlay";
import {ChevronDownIcon, ChevronRightIcon, ChevronUpIcon, XIcon} from "@primer/octicons-react";
import Popover from "react-bootstrap/Popover";

import {branches, commits} from '../../../rest/api';


const BranchSelector = ({ repo, selected, branches, listBranches, selectRef, withCommits, withWorkspace, amount = 300 }) => {

    // used for branch pagination
    const [pagination, setPagination] = useState({after: "", prefix: "", amount})

    // used for commit listing
    const initialCommitList = {branch: selected, commits: null, loading: false};
    const [commitList, setCommitList] = useState(initialCommitList);

    useEffect(()=> {
        listBranches(repo.id, pagination.prefix, pagination.after, pagination.amount)
    }, [repo.id, listBranches, pagination])

    const form = (
        <div className="ref-filter-form">
            <Form onSubmit={e => { e.preventDefault(); }}>
                <Form.Control type="text" placeholder="filter branches" onChange={(e)=> {
                    setPagination({
                        amount,
                        after: "",
                        prefix: e.target.value
                    })
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
                        <BranchEntry key={branch.id} repo={repo} branch={branch.id} selectRef={selectRef} selected={selected} withCommits={withCommits} logCommits={async () => {
                            const data = await commits.log(repo.id, branch.id)
                            setCommitList({...commitList, branch: branch.id, commits: data.results});
                        }}/>
                    ))}
                </ul>
                <Paginator results={branches.payload.results} pagination={branches.payload.pagination} from={pagination.after} onPaginate={(after) => {
                    setPagination({after})
                }}/>
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
            <h5>{branch}</h5>
            <div className="ref-scroller">
                <ul className="list-group ref-list">
                    {(withWorkspace) ? (
                        <li className="list-group-item" key={branch}>
                            <Button variant="link" onClick={() => {
                                selectRef({id: branch, type: 'branch'});
                            }}><em>{branch}'s Workspace (uncommitted changes)</em></Button>
                        </li>
                    ) : (<span/>)}
                    {commits.map(commit => (
                        <li className="list-group-item" key={commit.id}>
                            <Button variant="link" onClick={() => {
                                selectRef({id: commit.id, type: 'commit'});
                            }}>{getMessage(commit)} </Button>
                            <div className="actions">
                                <Badge variant="light">{commit.id.substr(0, 12)}</Badge>
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
        <li className="list-group-item" key={branch}>
            {(!!selected && branch === selected) ?
                <strong>{branch}</strong> :
                <Button variant="link" onClick={() => {
                    selectRef({id: branch, type: 'branch'});
                }}>{branch}</Button>
            }
            <div className="actions">
                {(branch === repo.default_branch) ? (<Badge variant="info">Default</Badge>) : <span/>}
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

    const [branchList, setBranches] = useState({loading: true, payload: null, error: null});

    const listBranches = useCallback(async (repoId, prefix, after, amount) => {
        setBranches({loading: true, payload: null, error: null});
        try {
            const response = await branches.list(repoId, prefix, after, amount);
            setBranches({loading: false, payload: response, error: null});
        } catch (error) {
            setBranches({loading: false, payload: null, error: error});
        }
    }, [])

    const popover = (
        <Overlay target={target.current} show={show} placement="bottom" rootClose={true} onHide={() => setShow(false)}>
            <Popover className="ref-popover">
                <Popover.Content>
                    <BranchSelector
                        repo={repo}
                        branches={branchList}
                        withCommits={withCommits}
                        listBranches={listBranches}
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
                    {emptyText} {show ? <ChevronUpIcon/> : <ChevronDownIcon/>}
                </Button>
                {cancelButton}
                {popover}
            </>
        );
    }

    const showId = (ref) => {
        if (!ref)
            return ''
        if (ref.type === 'commit')
            return ref.id.substr(0, 12)
        return ref.id
    }

    const title = prefix + (!!selected) ? `${prefix} ${selected.type}: ` : '';
    return (
        <>
            <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                {title} <strong>{showId(selected)}</strong> {show ? <ChevronUpIcon/> : <ChevronDownIcon/>}
            </Button>
            {cancelButton}
            {popover}
        </>
    );
};

export default RefDropdown;