import React, { FC, useContext } from "react";
import {UserHeader} from "../../../../lib/components/auth/nav";
import { GetUserEmailByIdContext } from "../index";

export interface UserHeaderWithContextProps {
    userId: string;
    page: string;
}

export const UserHeaderWithContext: FC<UserHeaderWithContextProps> = ({ userId, page }) => {
    const getUserEmailById = useContext(GetUserEmailByIdContext);
    const email = getUserEmailById(userId);
    return (
        <UserHeader userId={userId} userEmail={email} page={page} />
    );
};