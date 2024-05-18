import React, { useEffect, useState } from "react";

import Container from "react-bootstrap/Container";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Stack from "react-bootstrap/Stack";

import {useRefs} from "../../hooks/repo";
import { Outlet } from "react-router-dom";
import {RepositoryNavTabs} from "./tabs";
import {Link} from "../nav";
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
    return (
        <RefContextProvider>
            <div>
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
