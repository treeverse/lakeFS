import Layout from '../../../lib/components/layout';
import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryBranchesPage = () => {
    const router = useRouter()
    const { repoId } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(repoId)} activePage={'branches'}>
            <h1>branches</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryBranchesPage;