import React, {useEffect} from "react";

import {connect} from "react-redux";

import {Link} from "react-router-dom";
import Button from "react-bootstrap/Button";
import Octicon, {Sync} from "@primer/octicons-react";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Card from "react-bootstrap/Card";
import * as moment from "moment";

import {listRepositories} from "../actions/repositories";
import Table from "react-bootstrap/Table";
import Tooltip from "react-bootstrap/Tooltip";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";



const RepoList = ({ repositories, listRepositories }) => {

    useEffect(()=> {
        listRepositories();
    }, [listRepositories]);

    let content = (
        <h6 className="text-center">Loading...</h6>
    );
    if (!repositories.loading) {
        if (repositories.list.length > 0) {
            content = (
                <Table bordered hover>
                    <thead>
                    <tr>
                        <th>ID</th>
                        <th>Default Branch</th>
                        <th>Storage Namespace</th>
                        <th>Creation Date</th>
                    </tr>
                    </thead>
                    <tbody>
                    { repositories.list.map(repo => {
                        const created = moment.unix(repo.creation_date);
                        return (
                            <tr key={repo.id}>
                                <td><Link to={`/repositories/${repo.id}`}>{repo.id}</Link></td>
                                <td>{repo.default_branch}</td>
                                <td>{repo.bucket_name}</td>
                                <td>{created.format("MMMM Do YYYY, h:mm:ss a")} ({created.fromNow()})</td>
                            </tr>
                        );
                    })}
                    </tbody>
                </Table>
            );
        } else {
            // empty state
            content = (
                <h6 className="text-center">
                    You have no repositories configured. How about <Link to={"/repositories/create"}>creating one</Link>?
                </h6>
            )
        }
    }

    return (
        <Card>
            <Card.Header>
                <Link to={`/repositories`}>Repositories</Link>
            </Card.Header>
            <Card.Body>
                <ButtonToolbar className="justify-content-end mb-2" aria-label="Toolbar with Button groups">


                    <Button variant="outline-success" className="ml-2" as={Link} to={"/repositories/create"}>
                        Create Repository
                    </Button>

                    <OverlayTrigger placement="bottom" overlay={<Tooltip id="tooltip-refresh-repo-list">Refresh Repository List</Tooltip>}>
                        <Button variant="outline-primary" className="ml-2" onClick={(e) => {
                            e.preventDefault();
                            listRepositories();
                        }}>
                            <Octicon icon={Sync}/>
                        </Button>
                    </OverlayTrigger>

                </ButtonToolbar>
                <div className="mt-3">
                {content}
                </div>
            </Card.Body>
        </Card>
    );
};

export default connect(
    ({ repositories }) => ({repositories}),
    {listRepositories},
null)(RepoList);