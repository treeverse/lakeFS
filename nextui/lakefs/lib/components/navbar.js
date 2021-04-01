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

                <NavDropdown.Item
                    href="/auth/credentials"
                    onSelect={()=> router.push('/auth/credentials')}>
                        Manage Credentials
                </NavDropdown.Item>

            </NavDropdown>
        </Navbar.Collapse>
    );
}

const TopNavLink = ({ href, children }) => {
    const router = useRouter()
    const isActive = (prefix) => router.route.indexOf(prefix) === 0

    return (
        <Link href={href}>
            <Nav.Link href={href} active={isActive(href)}>{children}</Nav.Link>
        </Link>
    )
}

const TopNav = () => {
    return (
        <Navbar variant="dark" bg="dark" expand="md">
            <Link href="/">
                <Navbar.Brand href="/">
                    <img src="/logo.png" alt="lakeFS" className="logo"/>
                </Navbar.Brand>
            </Link>

            <Nav className="mr-auto">
                <TopNavLink href="/repositories">Repositories</TopNavLink>
                <TopNavLink href="/auth">Administration</TopNavLink>
            </Nav>

            <NavUserInfo/>
        </Navbar>
    )
}

export default TopNav