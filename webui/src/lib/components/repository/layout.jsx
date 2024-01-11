import React, { useCallback, useEffect, useState } from "react";
import { useLocalStorage } from "usehooks-ts";

import Container from "react-bootstrap/Container";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Stack from "react-bootstrap/Stack";

import {useRefs} from "../../hooks/repo";
import { Outlet } from "react-router-dom";
import {RepositoryNavTabs} from "./tabs";
import {Link} from "../nav";
import { config } from "../../api";
import { useAPI } from "../../hooks/api";
import RepoOnboardingChecklistSlider from "./repoOnboardingChecklistSlider";
import { RefContextProvider } from "../../hooks/repo";
import { ReadOnlyBadge } from "../badges";

const RepoNav = () => {
    const { repo } = useRefs();
    const [repoId, setRepoId] = useState("");
    useEffect(() => {
        if (repo) {
        setRepoId(repo.id);
        }
    }, [repo]);

    return (
        <Stack direction="horizontal" gap={2}>
            <Breadcrumb>
                <Link href={{pathname: '/repositories'}} component={Breadcrumb.Item}>
                    Repositories
                </Link>
                <Link href={{pathname: '/repositories/:repoId/objects', params: {repoId}}} component={Breadcrumb.Item}>
                    {repoId}
                </Link>
            </Breadcrumb>
            <ReadOnlyBadge readOnly={repo?.read_only} style={{ marginBottom: 16 }} />
        </Stack>
    );
};

export const RepositoryPageLayout = ({ fluid = "sm" }) => {
  const [activePage, setActivePage] = useState("objects");
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
        <RefContextProvider>
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
                    <div className="mt-4">
                      <Outlet context={[setActivePage]} />
                    </div>
                </Container>
            </div>
        </RefContextProvider>
    );
};
