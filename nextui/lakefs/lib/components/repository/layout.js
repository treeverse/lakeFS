import Layout from "../layout";

import Breadcrumb from "react-bootstrap/Breadcrumb";
import {RepositoryNavTabs} from "./tabs";
import {useRepoAndRef} from "../../hooks/repo";
import {Error, Loading} from "../controls";
import Link from 'next/link';

export const RepositoryPageLayout = ({ repoId, activePage, children }) => {
    return (
        <Layout>
            <div className="main-app container">
                <div className="mt-5">

                    <Breadcrumb>
                        <Breadcrumb.Item href={`/repositories`}>Repositories</Breadcrumb.Item>
                        <Breadcrumb.Item active href={`/repositories/${repoId}`}>{repoId}</Breadcrumb.Item>
                    </Breadcrumb>

                    <RepositoryNavTabs repoId={repoId} active={activePage}/>

                    <div className="mt-2">{children}</div>
                </div>
            </div>
        </Layout>
    )
}
