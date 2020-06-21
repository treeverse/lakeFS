import {connect} from "react-redux";
import React from "react";
import {Col} from "react-bootstrap";
import {UserCredentialsPane} from "./users";


export const CredentialsPage = connect(
    ({ auth }) => ({
        user: auth.user,
    }),
    ({  })
)(({  user }) => {

    const userId = user.id;

    return (
        <Col lg={9}>
            <UserCredentialsPane userId={userId}/>
        </Col>
    );
});
