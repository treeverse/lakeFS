import React, {FC, useCallback, useContext} from "react";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import {ClipboardButton} from "../../lib/components/controls";
import {useRouter} from "../../lib/hooks/router";
import noop from "lodash/noop";
import {AppContext} from "../../lib/hooks/appContext";
import {DownloadCredentialsButton} from "../../lib/components/auth/credentials";

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
    const {state} = useContext(AppContext);
    const buttonVariant = state.settings.darkMode ? "outline-light" : "outline-dark";

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
                <Col md={{offset: 3, span: 6}}>
                    <Card className="setup-widget">
                        <Card.Body className="after-setup-card">
                            <h2>You&apos;re all set!</h2>
                            <Card.Text>
                                Here are your credentials:<br/>
                            </Card.Text>
                            <div className="ms-2 row mt-4">
                                <div className="col-4">Access Key ID:</div>
                                <div className="col-8"><code>{accessKeyId}</code> &#160;&#160;<ClipboardButton
                                    onSuccess={noop} onError={noop} className={"copy-button"} variant={buttonVariant}
                                    text={accessKeyId} tooltip="Copy"/></div>
                            </div>
                            <div className="ms-2 row mt-2">
                                <div className="col-4">Secret Access Key:</div>
                                <div className="col-8"><code>{secretAccessKey}</code> &#160;&#160;<ClipboardButton
                                    onSuccess={noop} onError={noop} className={"copy-button"} variant={buttonVariant}
                                    text={secretAccessKey} tooltip="Copy"/></div>
                            </div>
                            <Alert className="mt-4" variant="warning">
                                This is the <strong>only</strong> time that the secret access keys can be viewed or
                                downloaded. You cannot recover them later.
                                <div className="mt-3 text-md-center">
                                    <DownloadCredentialsButton
                                        accessKeyId={accessKeyId}
                                        secretAccessKey={secretAccessKey}
                                        apiEndpoint={apiEndpoint}
                                    />
                                </div>
                            </Alert>
                            <h5>lakectl</h5>
                            <div className="ms-2 mt-2">
                                <a target="_blank" rel="noreferrer"
                                   href="https://docs.lakefs.io/reference/cli.html">lakectl</a> is a CLI tool for
                                working with lakeFS.
                                <p className="mt-2">
                                    Download lakectl as part of the <a target="_blank" rel="noreferrer"
                                                                       href="https://github.com/treeverse/lakeFS/releases">lakeFS
                                    release package</a> and save the above credentials file
                                    as <code>~/.lakectl.yaml</code>.
                                </p>
                            </div>
                            <div className="mt-3 text-md-center">
                                <Button className="p-2 pl-3 pr-3 after-setup-btn" onClick={goToLoginHandler}>Go To
                                    Login</Button>
                            </div>
                        </Card.Body>
                    </Card>
                </Col>
            </Row></>
    );
}
