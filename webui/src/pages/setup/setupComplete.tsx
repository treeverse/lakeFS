import React, {FC, useCallback} from "react";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import {DownloadIcon} from "@primer/octicons-react";
import {ClipboardButton} from "../../lib/components/controls";
import { useRouter } from "../../lib/hooks/router";
import noop from "lodash/noop";

interface SetupCompleteProps {
    accessKeyId: string;
    secretAccessKey: string;
    apiEndpoint: string;
}

export const SetupComplete: FC<SetupCompleteProps> = ({
    accessKeyId,
    secretAccessKey,
    apiEndpoint,
}) => {
    const router = useRouter();
    const downloadContent = 'data:application/octet-stream,' + encodeURIComponent(
        `# lakectl command line configuration - save under the filename $HOME/.lakectl.yaml
            credentials:
            access_key_id: ${accessKeyId}
            secret_access_key: ${secretAccessKey}
            server:
            endpoint_url: ${window.location.protocol}//${window.location.host}${apiEndpoint}
    `);
    
    const goToLoginHandler = useCallback(() => {
        let nextUrl = "/auth/login";
        // Need to refactor and convert the useRouter and useQuery hooks to TS in order to get rid of this any
        // If we weren't planning a TS conversion anyway, we could use Map in place of {}
        // or use the native URLSearchParams directly (which is more or less the same refactor from the call site perspective)
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (router.query && (router.query as any).next) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            nextUrl = `/auth/login?next=${(router.query as any).next}`;
        }
        window.open(nextUrl, "_blank")
    }, [router.query]);

    return (
        <>
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Body className="after-setup-card">
                        <h2>You&apos;re all set!</h2>
                        <Card.Text>
                            Here are the credentials for your first user:<br/>
                        </Card.Text>
                        <div className="ml-2 row mt-4">
                            <div className="col-3">Access Key ID:</div>
                            <div className="col-7"><code>{accessKeyId}</code> &#160;&#160;<ClipboardButton onSuccess={noop} onError={noop} className={"copy-button"} variant="outline-dark" text={accessKeyId} tooltip="Copy"/></div>
                        </div>
                        <div className="ml-2 row mt-2">
                            <div className="col-3">Secret Access Key:</div>
                            <div className="col-7"><code>{secretAccessKey}</code> &#160;&#160;<ClipboardButton onSuccess={noop} onError={noop} className={"copy-button"} variant="outline-dark" text={secretAccessKey} tooltip="Copy"/></div>
                        </div>
                        <Alert className="mt-4" variant="warning">
                            This is the <strong>only</strong> time that the secret access keys can be viewed or downloaded. You cannot recover them later.
                        </Alert>
                        <div className="mt-4" style={{textAlign: "center"}}>
                            <Button className="p-2 pl-3 pr-3 after-setup-btn" onClick={goToLoginHandler}>Go To Login</Button>
                        </div>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
            <Row>
                <Col md={{offset: 2, span: 8}}>
                    <Card className="mt-5">
                        <Card.Body className="after-setup-card">
                            <h5>Configure lakectl</h5>
                            <div>Use the command-line tool to perform Git-like operations on your data. Save the configuration file under <code>~/.lakectl.yaml</code>:</div>
                            <div className="mt-3" style={{textAlign: "center"}}>
                                <a className="btn p-2 pl-3 pr-3 after-setup-btn" href={downloadContent}
                                        target="_blank" rel="noreferrer" download="lakectl.yaml"><DownloadIcon/> Download Configuration
                                </a>
                            </div>
                            <div className="mt-3">
                                To use lakectl, you will need to download the binary. <a href="https://www.youtube.com/watch?v=8nO7RT411nA" rel="noreferrer" target={"_blank"}>Learn more</a>.
                            </div>
                        </Card.Body>
                    </Card>
                </Col>
            </Row></>
    );
}