import Link from 'next/link';

import Breadcrumb from "react-bootstrap/Breadcrumb";

import {RepositoryNavTabs} from "./tabs";
import Layout from "../layout";

export const RepositoryPageLayout = ({ repoId, activePage, children }) => {
    return (
        <Layout>
            <div className="main-app container">
                <div className="mt-5">

                    <Breadcrumb>
                        <Link passHref href={{pathname: '/repositories'}}>
                            <Breadcrumb.Item>Repositories</Breadcrumb.Item>
                        </Link>
                        <Link passHref href={{pathname: '/repositories/[repoId]/objects', query: {repoId}}}>
                            <Breadcrumb.Item active>{repoId}</Breadcrumb.Item>
                        </Link>
                    </Breadcrumb>

                    <RepositoryNavTabs repoId={repoId} active={activePage}/>

                    <div className="mt-2">{children}</div>
                </div>
            </div>
        </Layout>
    )
}
