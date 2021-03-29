import Layout from "../layout";

import Breadcrumb from "react-bootstrap/Breadcrumb";
import {RepositoryNavTabs} from "./tabs";

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

                    <>
                        {children}
                    </>
                </div>
            </div>
        </Layout>
    )
}