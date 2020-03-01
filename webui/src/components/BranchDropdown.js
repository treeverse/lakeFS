import React, {useState, useRef, useEffect} from 'react';
import {connect} from "react-redux";

import Popover from "react-bootstrap/Popover";
import Overlay from "react-bootstrap/Overlay";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import Button from "react-bootstrap/Button";
import Octicon, {ChevronDown} from "@primer/octicons-react";

import {listBranches} from "../actions/objects";
import Form from "react-bootstrap/Form";


const BranchSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {

    useEffect(()=> {
        listBranches(repoId, "", 5);
    }, [listBranches, repoId]);

   return (
        <div>
            <Form onSubmit={e => { e.preventDefault(); }}>
                <Form.Control type="text" placeholder="filter branches" autoFocus onChange={(e)=> {
                    listBranches(repoId, e.currentTarget.value, 5);
                }}/>
            </Form>
            {(!!!branches.payload || branches.loading) ?
                <p>Loading...</p> :
                <ul className={"list-group"}>
                    {branches.payload.results.map(branch => (
                        <li className="list-group-item" key={branch.id}>
                            <Button variant="link" onClick={(e) => {
                                selectRef({id: branch.id, type: 'BRANCH'});
                            }}>{branch.id}</Button>
                        </li>
                    ))}
                </ul>
            }
        </div>
    );
};

const CommitSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {
    return (
        <p>Commits...</p>
    );
};


const RefSelector = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {
    const [key, setKey] = useState('branches');

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



const BranchDropdown = ({ repoId, selectedBranch, branches,  listBranches, selectRef }) => {
    const [show, setShow] = useState(false);
    const target = useRef(null);

    return (
        <>
            <Button ref={target} variant="light" onClick={()=> { setShow(!show) }}>
                Branch: <strong>{selectedBranch}</strong> <Octicon icon={ChevronDown}/>
            </Button>
            <Overlay target={target.current} show={show} placement="bottom">
                <Popover>
                    <Popover.Content>
                        <RefSelector repoId={repoId} selectedBranch={selectedBranch} branches={branches} listBranches={listBranches} selectRef={selectRef}/>
                    </Popover.Content>
                </Popover>
            </Overlay>
        </>
    );
}
export default connect(
    ({ objects }) => ({ branches: objects.branches }),
    ({ listBranches })
)(BranchDropdown);