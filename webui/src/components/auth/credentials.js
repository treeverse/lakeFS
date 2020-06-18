import {connect} from "react-redux";
import {createCredentials, resetCreateCredentials} from "../../actions/auth";
import React, {useCallback} from "react";
import {EntityCreateButton} from "./entities";
import {Alert, Col, Table} from "react-bootstrap";
import ClipboardButton from "../ClipboardButton";
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
