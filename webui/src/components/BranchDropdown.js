import React, {useState, useRef, useEffect} from 'react';
import {connect} from "react-redux";

import Popover from "react-bootstrap/Popover";
import Overlay from "react-bootstrap/Overlay";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import Button from "react-bootstrap/Button";
import Octicon, {ChevronDown, ChevronUp} from "@primer/octicons-react";

import {listBranches} from "../actions/branches";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";


const BranchSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {

    useEffect(()=> {
        listBranches(repoId, "", 5);
    }, [listBranches, repoId]);

    const form = (
        <Form onSubmit={e => { e.preventDefault(); }}>
            <Form.Control type="text" placeholder="filter branches" autoFocus onChange={(e)=> {
                listBranches(repoId, e.currentTarget.value, 5);
            }}/>
        </Form>
    );

    if (branches.loading) {
        return  (
            <div className="ref-selector">
                {form}
                <p>Loading...</p>
            </div>
        );
    }

   return (
        <div className="ref-selector">
            {form}
            {(!!branches.error) ?
                <Alert variant="danger">{branches.error}</Alert> :
                <ul className={"list-group"}>
                    {branches.payload.results.map(branch => (
                        <li className="list-group-item" key={branch.id}>
                            {(branch.id === selectedBranch) ?
                                <strong>{branch.id}</strong> :
                                <Button variant="link" onClick={(e) => {
                                    selectRef({id: branch.id, type: 'BRANCH'});
                                }}>{branch.id}</Button>
                            }
                        </li>
                    ))}
                </ul>
            }
        </div>
    );
};

const CommitSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {
    return (
        <div className="ref-selector">
            commits for {selectedBranch}
        </div>
    );
};


const RefSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef, withCommits }) => {
    const [key, setKey] = useState('branches');

    if (!withCommits) {
        return (
            <BranchSelector repoId={repoId} selectedBranch={selectedBranch} branches={branches} listBranches={listBranches} selectRef={selectRef}/>
        );
    }

    return (
        <Tabs activeKey={key} onSelect={k => setKey(k)}>
            <Tab eventKey="branches" title="Branches">
                <BranchSelector repoId={repoId} selectedBranch={selectedBranch} branches={branches} listBranches={listBranches} selectRef={selectRef}/>
            </Tab>
            <Tab eventKey="commits" title="Commits">
                <CommitSelector repoId={repoId} selectedBranch={selectedBranch} branches={branches} listBranches={listBranches} selectRef={selectRef}/>
            </Tab>
        </Tabs>
    );
};



const BranchDropdown = ({ repoId, selectedBranch, branches,  listBranches, selectRef, withCommits = true }) => {
    const [show, setShow] = useState(false);
    const target = useRef(null);

    return (
        <>
            <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                Branch: <strong>{selectedBranch}</strong> <Octicon icon={show ? ChevronUp : ChevronDown}/>
            </Button>
            <Overlay target={target.current} show={show} placement="bottom">
                <Popover>
                    <Popover.Content>
                        <RefSelector repoId={repoId} selectedBranch={selectedBranch} branches={branches} withCommits={withCommits} listBranches={listBranches} selectRef={(ref) => {
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
    ({ listBranches })
)(BranchDropdown);