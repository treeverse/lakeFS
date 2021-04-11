import Link from 'next/link';

import Breadcrumb from "react-bootstrap/Breadcrumb";

import {RepositoryNavTabs} from "./tabs";
import Layout from "../layout";
import Container from "react-bootstrap/Container";
import {useRefs} from "../../hooks/repo";

const RepoNav = () => {

    const { repo } = useRefs()
    const repoId = (!!repo) ? repo.id : '';

    return (
        <Breadcrumb>
            <Link passHref href={{pathname: '/repositories'}}>
                <Breadcrumb.Item>Repositories</Breadcrumb.Item>
            </Link>
            <Link passHref href={{pathname: '/repositories/[repoId]/objects', query: {repoId}}}>
                <Breadcrumb.Item active>{repoId}</Breadcrumb.Item>
            </Link>
        </Breadcrumb>

    )
}

export const RepositoryPageLayout = ({ activePage, children, fluid = "xl" }) => {
    return (
        <Layout>
            <div>

                <RepoNav/>

                <RepositoryNavTabs active={activePage}/>

                <Container fluid={fluid}>
                    <div className="mt-2">{children}</div>
                </Container>
            </div>
        </Layout>
    )
}
