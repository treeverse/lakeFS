import Layout from '../../../lib/components/layout';
import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryComparePage = () => {
    const router = useRouter()
    const { repoId } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(repoId)} activePage={'compare'}>
            <h1>compare</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryComparePage;