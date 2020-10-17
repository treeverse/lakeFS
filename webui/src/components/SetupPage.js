import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Card from "react-bootstrap/Card";
import ClipboardButton from "./ClipboardButton";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Table from "react-bootstrap/Table";
import React, {useRef} from "react";
import Row from "react-bootstrap/Row";
import { DownloadIcon } from "@primer/octicons-react";
import {connect} from "react-redux";
import {doSetupLakeFS} from "../actions/setup";
import {API_ENDPOINT} from "../actions/api";

const SetupPage = ({ doSetupLakeFS, setupState }) => {
    const usernameRef = useRef(null);

    const disabled = setupState.loading;

    const onSubmit = (event) => {
        if (disabled) return;
        doSetupLakeFS(usernameRef.current.value);
        event.preventDefault();
    };

    if (!!setupState.payload && setupState.payload.access_key_id) {
        const downloadContent = 'data:application/octet-stream,' + encodeURIComponent(
`# lakectl command line configuration - save under the filename $HOME/.lakectl.yaml

credentials:
  access_key_id: ${setupState.payload.access_key_id}
  secret_access_key: ${setupState.payload.secret_access_key}
server:
  endpoint_url: ${window.location.protocol}//${window.location.host}${API_ENDPOINT}
`);
        return (
            <Card className="setup-widget">
                <Card.Header>Congratulations</Card.Header>
                <Card.Body>
                    <Card.Text>
                        Database was initialized and admin user was created.<br/>
                        Here are your credentials:<br/>
                    </Card.Text>
                    <hr/>
                    <Table borderless hover>
                        <tbody>
                            <tr>
                                <td>Key ID</td>
                                <td><code>{setupState.payload.access_key_id}</code> <ClipboardButton variant="link" text={setupState.payload.access_key_id} tooltip="Copy key ID"/></td>
                            </tr>
                            <tr>
                                <td>Secret Key</td>
                                <td><code>{setupState.payload.secret_access_key}</code> <ClipboardButton variant="link" text={setupState.payload.secret_access_key} tooltip="Copy secret key"/></td>
                            </tr>
                        </tbody>
                    </Table>
                    <Card.Text>
                        Download the initial client configuration under your <code>$HOME/.lakectl.yaml</code> and keep a copy of the data for your login into the system<br/>
                    </Card.Text>
                    <Alert variant="warning">
                        This is the <strong>only</strong> time that the secret access keys can be viewed or downloaded. You cannot recover them later.
                    </Alert>
                    <hr/>
                    <ButtonToolbar>
                        <Button variant="success" href={downloadContent} taget="_blank" download="lakectl.yaml"><DownloadIcon /> Download Configuration</Button>{' '}
                        <Button variant="link" href="/login">Go To Login</Button>
                    </ButtonToolbar>
                </Card.Body>
            </Card>
        );
    }
    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Header>Initial Setup</Card.Header>
                    <Card.Body>
                        <Card.Text>
                            This process will initialize the database schema and a first admin user to access the system.<br/>
                            Enter an admin username.
                        </Card.Text>
                        <Form onSubmit={onSubmit}>
                            <Form.Group controlId="user-name">
                                <Form.Control type="text" placeholder="Username" ref={usernameRef} autoFocus/>
                            </Form.Group>

                            {setupState.error && <Alert variant={"danger"}>{setupState.error}</Alert>}
                            <Button variant="primary" disabled={disabled} type="submit">Setup</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};

export default connect(
    ({ setup }) => ({ setupState: setup.setupLakeFS }),
    ({ doSetupLakeFS })
)(SetupPage);
