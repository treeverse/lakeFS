import React, { FC, useContext } from "react";
import {UserHeader} from "../../../../lib/components/auth/nav";
import { GetUserDisplayNameByIdContext } from "../index";

export interface UserHeaderWithContextProps {
    userId: string;
    page: string;
}

export const UserHeaderWithContext: FC<UserHeaderWithContextProps> = ({ userId, page }) => {
    const getUserDisplayNameById = useContext(GetUserDisplayNameByIdContext);
    const userDisplayName = getUserDisplayNameById(userId);
    return (
        <UserHeader userId={userId} userDisplayName={userDisplayName} page={page} />
    );
};