import React, { useCallback, useEffect, useState } from "react";
import { useLocalStorage } from "usehooks-ts";

import Container from "react-bootstrap/Container";
import Breadcrumb from "react-bootstrap/Breadcrumb";

import {useRefs} from "../../hooks/repo";
import Layout from "../layout";
import {RepositoryNavTabs} from "./tabs";
import {Link} from "../nav";
import { config } from "../../api";
import { useAPI } from "../../hooks/api";
import RepoOnboardingChecklistSlider from "./repoOnboardingChecklistSlider";

const RepoNav = () => {
    const { repo } = useRefs();
    const repoId = (repo) ? repo.id : '#';
    
    return (
        <Breadcrumb>
            <Link href={{pathname: '/repositories'}} component={Breadcrumb.Item}>
                Repositories
            </Link>
            <Link href={{pathname: '/repositories/:repoId/objects', params: {repoId}}} component={Breadcrumb.Item}>
                {repoId}
            </Link>
        </Breadcrumb>

    )
};

export const RepositoryPageLayout = ({ activePage, children, fluid = "sm" }) => {
    const [showChecklist, setShowChecklist] = useLocalStorage(
        "showChecklist",
        false
    );
    const [dismissedChecklistForRepo, setDismissedChecklistForRepo] =
        useLocalStorage(`dismissedChecklistForRepo`, false);
    const [configRes, setConfigRes] = useState(null);
    const { response } = useAPI(() => {
        return config.getStorageConfig();
    }, []);

    const dismissChecklist = useCallback(() => {
        setShowChecklist(false);
        setTimeout(() => setDismissedChecklistForRepo(true), 700);
    }, [setDismissedChecklistForRepo]);

    useEffect(() => {
        if (response) {
        setConfigRes(response);
        }
    }, [response, setConfigRes]);

    return (
        <Layout>
            <div>
                {configRes && !dismissedChecklistForRepo && (
                    <RepoOnboardingChecklistSlider
                        show={showChecklist}
                        showChecklist={setShowChecklist}
                        blockstoreType={configRes.blockstore_type}
                        dismissChecklist={dismissChecklist}
                    />
                )}
                <RepoNav/>

                <RepositoryNavTabs active={activePage}/>

                <Container fluid={fluid}>
                    <div className="mt-4">{children}</div>
                </Container>
            </div>
        </Layout>
    );
};