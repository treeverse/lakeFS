import Link from "next/link";
import Nav from "react-bootstrap/Nav";


export const NavItem = ({ href, active, children }) => {
    return (
        <Nav.Item>
            <Link href={href} passHref>
                <Nav.Link active={active}>
                    <>{children}</>
                </Nav.Link>
            </Link>
        </Nav.Item>
    );
};