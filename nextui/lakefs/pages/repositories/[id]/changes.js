import Layout from '../../../lib/components/layout';
import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryChangesPage = () => {
    const router = useRouter()
    const { id } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(id)} activePage={'changes'}>
            <h1>changes</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryChangesPage;