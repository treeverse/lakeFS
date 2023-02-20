import React, {useCallback, useEffect, useRef} from "react";
import {useState} from "react";
import {API_ENDPOINT, setup, SETUP_STATE_NOT_INITIALIZED, SETUP_STATE_INITIALIZED, SETUP_STATE_COMMUNICATION_PERFS_DONE} from "../../lib/api";
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";
import {SetupComplete} from "./setupComplete";
import {AdminUserSetup} from "./adminUser";
import {CommunicationPreferencesSetup} from "./communicationPreferences";


const SetupContents = () => {
    const usernameRef = useRef(null);
    const [setupError, setSetupError] = useState(null);
    const [setupData, setSetupData] = useState(null);
    const [disabled, setDisabled] = useState(false);
    const [currentStep, setCurrentStep] = useState(null);
    const router = useRouter();
    const { response, error, loading } = useAPI(() => {
        return setup.getState()
    });

    useEffect(() => {
        // Set initial state
        if (!error && response) {
            setCurrentStep(response?.state);
        }
    }, [error, response]);

    const onSubmitAdminUser = useCallback(async () => {
        setDisabled(true);
        try {
            const response = await setup.lakeFS(usernameRef.current.value);
            setSetupError(null);
            setSetupData(response);
        } catch (error) {
            setSetupError(error);
            setSetupData(null);
        } finally {
            setDisabled(false);
        }
    }, [setDisabled, setSetupError, setSetupData, setup]);

    const onSubmitCommunicationPreferences = useCallback(async (adminUser, userEmail, updatesChecked, securityChecked) => {
        if (!userEmail) {
            setSetupError("Please enter your email address.");
            return;
        }
        setDisabled(true);
        try {
            await setup.commPrefs(userEmail, updatesChecked, securityChecked);
            const response = await setup.lakeFS(adminUser);
            setSetupError(null);
            setSetupData(response);
        } catch (error) {
            setSetupError(error);
        } finally {
            setDisabled(false);
        }
    }, [setDisabled, setSetupError, setup]);

    if (loading) {
        return null;
    }

    if (setupData && setupData.access_key_id) {
        return (
            <SetupComplete
                accessKeyId={setupData.access_key_id}
                secretAccessKey={setupData.secret_access_key}
                apiEndpoint={API_ENDPOINT}
            />
        );
    }

    
    switch (currentStep) {
        case SETUP_STATE_INITIALIZED:
            return router.push({pathname: '/', query: router.query});
        case SETUP_STATE_COMMUNICATION_PERFS_DONE:
            return (
                <AdminUserSetup
                    onSubmit={onSubmitAdminUser}
                    setupError={setupError}
                    disabled={disabled}
                    ref={usernameRef}
                />
            );
        case SETUP_STATE_NOT_INITIALIZED:
            return (
                <CommunicationPreferencesSetup
                    onSubmit={onSubmitCommunicationPreferences}
                    setupError={setupError}
                    disabled={disabled}
                />
            );
        default:
            return null;
    }
    
};


const SetupPage = () => <SetupContents/>;

export default SetupPage;
