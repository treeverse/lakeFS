import React from "react";

import Container from "react-bootstrap/Container";
import Breadcrumb from "react-bootstrap/Breadcrumb";

import {useRefs} from "../../hooks/repo";
import Layout from "../layout";
import {RepositoryNavTabs} from "./tabs";
import {Link} from "../nav";


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
    return (
        <Layout>
            <div>

                <RepoNav/>

                <RepositoryNavTabs active={activePage}/>

                <Container fluid={fluid}>
                    <div className="mt-4">{children}</div>
                </Container>
            </div>
        </Layout>
    );
};
