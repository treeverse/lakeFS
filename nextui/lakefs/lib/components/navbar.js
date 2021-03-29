import Navbar from 'react-bootstrap/Navbar'
import Nav from 'react-bootstrap/Nav'
import NavDropdown from 'react-bootstrap/NavDropdown'

import {useRouter} from 'next/router'
import Link from 'next/link'
import useUser from '../hooks/user'


const NavUserInfo = () => {
    const router = useRouter();
    const { user } = useUser();
    if (!user || !user.loggedIn)
        return (<></>);
    const details = user.details;
    return (
        <Navbar.Collapse className="justify-content-end">
            <NavDropdown title={details.id} className="navbar-username" alignRight>
                <NavDropdown.Header>
                    Access Key ID: <code>{details.accessKeyId}</code>
                </NavDropdown.Header>

                <NavDropdown.Divider/>

                <NavDropdown.Item onSelect={()=> { router.push('/auth/credentials') }}>Manage Credentials</NavDropdown.Item>

            </NavDropdown>
        </Navbar.Collapse>
    );
}

const TopNav = () => {

    const router = useRouter()
    const isActive = (prefix) => router.route.indexOf(prefix) === 0

    return (
        <Navbar variant="dark" bg="dark" expand="md">
            <Navbar.Brand href="/">
                <img src="/logo.png" alt="lakeFS" className="logo"/>
            </Navbar.Brand>

            <Nav className="mr-auto">
                <Link href="/repositories">
                    <Nav.Link active={isActive("/repositories")}>Repositories</Nav.Link>
                </Link>
                <Link href="/auth">
                    <Nav.Link active={isActive("/auth")}>Administration</Nav.Link>
                </Link>
            </Nav>

            <NavUserInfo/>
        </Navbar>
    )
}

export default TopNav