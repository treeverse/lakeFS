import Container from 'react-bootstrap/Container'
import TopNav from './navbar';

const Layout = ({ children }) => {
    return (
        <>
            <TopNav/>
            <Container fluid={"xl"} className="main-app">
                {children}
            </Container>
        </>
    )
}

export default Layout;