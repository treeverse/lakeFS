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
import {setup, API_ENDPOINT, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {ClipboardButton, Error} from "../../lib/components/controls";
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";


const SetupContents = () => {
    const usernameRef = useRef(null);
    const [setupError, setSetupError] = useState(null);
    const [setupData, setSetupData] = useState(null);
    const [disabled, setDisabled] = useState(false);
    const router = useRouter();
    const { response, error, loading } = useAPI(() => {
        return setup.getState()
    });
    if (loading) {
        return null;
    }
    if (!error && response && response.state === SETUP_STATE_INITIALIZED) {
        router.push({pathname: '/', query: router.query})
    }
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
            <>
            <Row>
                <Col md={{offset: 2, span: 8}}>
                    <Card className="setup-widget">
                        <Card.Body className={"after-setup-card"}>
                            <h2>You&apos;re all set!</h2>
                            <Card.Text>
                                Here are the credentials for your first user:<br/>
                            </Card.Text>
                            <div className={"ml-2 row mt-4"}>
                                <div className={"col-3"}>Access Key ID:</div>
                                <div className={"col-7"}><code>{setupData.access_key_id}</code> &#160;&#160;<ClipboardButton className={"copy-button"} variant="outline-dark" text={setupData.access_key_id} tooltip="Copy"/></div>
                            </div>
                            <div className={"ml-2 row mt-2"}>
                                <div className={"col-3"}>Secret Access Key:</div>
                                <div className={"col-7"}><code>{setupData.secret_access_key}</code> &#160;&#160;<ClipboardButton className={"copy-button"} variant="outline-dark" text={setupData.secret_access_key} tooltip="Copy"/></div>
                            </div>
                            <Alert className={"mt-4"} variant="warning">
                                This is the <strong>only</strong> time that the secret access keys can be viewed or downloaded. You cannot recover them later.
                            </Alert>
                            <div className={"mt-4"} style={{textAlign: "center"}}>
                                <Button className={"p-2 pl-3 pr-3 after-setup-btn"} onClick={() => window.open(router.query && router.query.next ? `/auth/login?next=${router.query.next}` : '/auth/login', "_blank")}>Go To Login</Button>
                            </div>
                        </Card.Body>
                    </Card>
                </Col>
            </Row>
                <Row>
                    <Col md={{offset: 2, span: 8}}>
                        <Card className="mt-5">
                            <Card.Body className={"after-setup-card"}>
                                <h5>Configure lakectl</h5>
                                <div>Use the command-line tool to perform Git-like operations on your data. Save the configuration file under <code>~/.lakectl.yaml</code>:</div>
                                <div className={"mt-3"} style={{textAlign: "center"}}>
                                    <Button className={"p-2 pl-3 pr-3 after-setup-btn"} href={downloadContent}
                                            taget="_blank" download="lakectl.yaml"><DownloadIcon/> Download Configuration
                                    </Button>
                                </div>
                                <div className={"mt-3"}>
                                    To use lakectl, you will need to download the binary. <a href="https://www.youtube.com/watch?v=8nO7RT411nA" rel="noreferrer" target={"_blank"}>Learn more</a>.
                                </div>
                            </Card.Body>
                        </Card>
                    </Col>
                </Row></>
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
                            <div><a href="https://docs.lakefs.io/quickstart/repository.html#create-the-first-user" target="_blank" rel="noopener noreferrer">Learn more.</a></div>
                        </Card.Text>
                        <Form onSubmit={e => {
                            onSubmit();
                            e.preventDefault();
                        }}>
                            <Form.Group controlId="user-name">
                                <Form.Control type="text" placeholder="Admin Username" ref={usernameRef} autoFocus/>
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


const SetupPage = () => <SetupContents/>;

export default SetupPage;
