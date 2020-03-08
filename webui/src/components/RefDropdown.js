import React, {useState, useRef, useEffect} from 'react';
import {connect} from "react-redux";

import Popover from "react-bootstrap/Popover";
import Overlay from "react-bootstrap/Overlay";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import Button from "react-bootstrap/Button";
import Octicon, {ChevronDown, ChevronUp} from "@primer/octicons-react";

import {filterBranches, listBranches} from "../actions/branches";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Badge from "react-bootstrap/Badge";


const arrayMove = (arr, oldIndex, newIndex) => {
    while (oldIndex < 0) {
        oldIndex += arr.length;
    }
    while (newIndex < 0) {
        newIndex += arr.length;
    }
    if (newIndex >= arr.length) {
        let k = newIndex - arr.length + 1;
        while (k--) {
            arr.push(undefined);
        }
    }
    arr.splice(newIndex, 0, arr.splice(oldIndex, 1)[0]);
};

const hoistDefaultBranch = (branches, repo) => {
    let defaultIndex = -1;
    const branchesWithDefault = branches.map((branch, i) => {
        const isDefault = (branch.id === repo.default_branch);
        if (isDefault) defaultIndex = i;
        return { ...branch,  default: isDefault};
    });
    // yank the default branch and stick it first
    if (defaultIndex !== -1) {
        arrayMove(branchesWithDefault, defaultIndex, 0);
    }
    return branchesWithDefault;
};

const BranchSelector = ({ repo, selected, branches, filterBranches, listBranches, selectRef, amount = 1000 }) => {

    const [from, setFrom] = useState("");

    useEffect(()=> {
        listBranches(repo.id, from, amount);
    }, [listBranches,  repo.id, amount, from]);

    const form = (
        <div className="ref-filter-form">
            <Form onSubmit={e => { e.preventDefault(); }}>
                <Form.Control type="text" placeholder="filter branches" autoFocus onChange={(e)=> {
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

    const results = hoistDefaultBranch(branches.payload.results, repo);

    return (
        <div className="ref-selector">
            {form}
            <div className="ref-scroller">
                <ul className="list-group ref-list">
                    {results.map(branch => (
                        <li className="list-group-item" key={branch.id}>
                            {(selected.type === 'branch' && branch.id === selected.id) ?
                                <strong>{branch.id}</strong> :
                                <Button variant="link" onClick={() => {
                                    selectRef({id: branch.id, type: 'branch'});
                                }}>{branch.id}</Button>
                            }
                            {(branch.default) ? (<Badge variant="light">Default</Badge>) : <span/>}
                        </li>
                    ))}
                </ul>
                <Paginator results={branches.payload.results} pagination={branches.payload.pagination} from={from} onPaginate={setFrom}/>
            </div>
        </div>
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

const CommitSelector = ({ repo, selectedBranch, branches, selectRef }) => {
    return (
        <div className="ref-selector">
            commits for {selectedBranch}
        </div>
    );
};

// (RefSelector encapsulates BranchSelector and CommitSelector and is not currently used)
// eslint-disable-next-line
const RefSelector = ({ repo, selectedBranch, branches,  filterBranches, selectRef, withCommits }) => {
    const [key, setKey] = useState('branches');

    if (!withCommits) {
        return (
            <BranchSelector repo={repo} selectedBranch={selectedBranch} branches={branches} filterBranches={filterBranches} selectRef={selectRef}/>
        );
    }

    return (
        <Tabs activeKey={key} onSelect={k => setKey(k)}>
            <Tab eventKey="branches" title="Branches">
                <BranchSelector repo={repo} selectedBranch={selectedBranch} branches={branches} filterBranches={filterBranches} selectRef={selectRef}/>
            </Tab>
            <Tab eventKey="commits" title="Commits">
                <CommitSelector repo={repo} selectedBranch={selectedBranch} branches={branches}  selectRef={selectRef}/>
            </Tab>
        </Tabs>
    );
};


const RefDropdown = ({ repo, selected, branches,  filterBranches, listBranches, selectRef, withCommits = true }) => {
    const [show, setShow] = useState(false);
    const target = useRef(null);

    const title = (selected.type === 'branch') ? 'Branch: ' : 'Commit: ';

    return (
        <>
            <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                {title} <strong>{selected.id}</strong> <Octicon icon={show ? ChevronUp : ChevronDown}/>
            </Button>
            <Overlay target={target.current} show={show} placement="bottom">
                <Popover className="ref-popover">
                    <Popover.Content>
                        <BranchSelector repo={repo} selected={selected} branches={branches} withCommits={withCommits} listBranches={listBranches} filterBranches={filterBranches} selectRef={(ref) => {
                            selectRef(ref);
                            setShow(false);
                        }}/>
                    </Popover.Content>
                </Popover>
            </Overlay>
        </>
    );
}
export default connect(
    ({ branches }) => ({ branches: branches.list }),
    ({ filterBranches, listBranches })
)(RefDropdown);