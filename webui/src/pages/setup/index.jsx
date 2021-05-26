import React, {useRef} from "react";

import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Table from "react-bootstrap/Table";
import Row from "react-bootstrap/Row";
import {DownloadIcon} from "@primer/octicons-react";
import {useState} from "react";
import {setup, warnings, API_ENDPOINT} from "../../lib/api";
import {ClipboardButton, Error, Warnings} from "../../lib/components/controls";
import {useAPI} from "../../lib/hooks/api";


const useWarnings = () => {
    const { response, loading, error } = useAPI(async () => {
	let ret = await warnings.get();
	return ret && ret.JSON200 && ret.JSON200.warnings;
    }, []);
    return { warnings: response, loading, error }
};

const SetupWarnings = () => {
    const { warnings, error } = useWarnings();

    const errorOrWarnings = error ? [`Cannot fetch warnings: ${error}`] : warnings;

    return <Warnings warnings={errorOrWarnings}/>;
};

const SetupContents = () => {
    const usernameRef = useRef(null);
    const [setupError, setSetupError] = useState(null);
    const [setupData, setSetupData] = useState(null);
    const [disabled, setDisabled] = useState(false);

    const onSubmit = async () => {
        setDisabled(true);
        try {
            const response = await setup.lakeFS(usernameRef.current.value);
            setSetupError(null);
            setSetupData(response);
        } catch (error) {
            setSetupError(error);
            setSetupData(null);
        } finally {
            setDisabled(false);
        }
    };

    if (setupData && setupData.access_key_id) {
        const downloadContent = 'data:application/octet-stream,' + encodeURIComponent(
            `# lakectl command line configuration - save under the filename $HOME/.lakectl.yaml
credentials:
  access_key_id: ${setupData.access_key_id}
  secret_access_key: ${setupData.secret_access_key}
server:
  endpoint_url: ${window.location.protocol}//${window.location.host}${API_ENDPOINT}
`);
        return (
            <Row>
                <Col md={{offset: 2, span: 8}}>
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
                                    <td><code>{setupData.access_key_id}</code> <ClipboardButton variant="link" text={setupData.access_key_id} tooltip="Copy key ID"/></td>
                                </tr>
                                <tr>
                                    <td>Secret Key</td>
                                    <td><code>{setupData.secret_access_key}</code> <ClipboardButton variant="link" text={setupData.secret_access_key} tooltip="Copy secret key"/></td>
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
                            <Button variant="success" href={downloadContent} taget="_blank" download="lakectl.yaml"><DownloadIcon />Download Configuration </Button>
                            {' '}
                            <Button variant="link" href="/">Go To Login</Button>
                        </Card.Body>
                    </Card>
                </Col>
            </Row>
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
                        <Form onSubmit={e => {
                            onSubmit();
                            e.preventDefault();
                        }}>
                            <Form.Group controlId="user-name">
                                <Form.Control type="text" placeholder="Username" ref={usernameRef} autoFocus/>
                            </Form.Group>

                            {!!setupError && <Error error={setupError}/>}
                            <Button variant="primary" disabled={disabled} type="submit">Setup</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};

const SetupPage = () => <>
			    <SetupWarnings/>
			    <SetupContents/>
			</>;

export default SetupPage;
