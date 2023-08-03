import React, {useCallback, useEffect} from "react";
import Layout from "../../lib/components/layout";
import {useState} from "react";
import {API_ENDPOINT, setup, SETUP_STATE_NOT_INITIALIZED, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";
import {SetupComplete} from "./setupComplete";
import {UserConfiguration} from "./userConfiguration";


const SetupContents = () => {
    const [setupError, setSetupError] = useState(null);
    const [setupData, setSetupData] = useState(null);
    const [disabled, setDisabled] = useState(false);
    const [currentStep, setCurrentStep] = useState(null);
    const [missingCommPrefs, setMissingCommPrefs] = useState(null);
    const router = useRouter();
    const { response, error, loading } = useAPI(() => {
        return setup.getState()
    });

    useEffect(() => {
        // Set initial state
        if (!error && response) {
            setCurrentStep(response.state);
            setMissingCommPrefs(response.state !== SETUP_STATE_INITIALIZED || response.comm_prefs_done === false);
        }
    }, [error, response]);

    const onSubmitUserConfiguration = useCallback(async (adminUser, userEmail, checked) => {
        if (currentStep !== SETUP_STATE_NOT_INITIALIZED && !adminUser) {
            setSetupError("Please enter your admin username.");
            return;
        }
        if (!userEmail) {
            setSetupError("Please enter your email address.");
            return;
        }

        setDisabled(true);
        try {
            if (missingCommPrefs) {
               await setup.commPrefs(userEmail, checked, checked);
               setMissingCommPrefs(false);
            }
            if (currentStep !== SETUP_STATE_INITIALIZED) {
                const response = await setup.lakeFS(adminUser);
                setSetupData(response);
            }
            setSetupError(null);
        } catch (error) {
            setSetupError(error);
        } finally {
            setDisabled(false);
        }
    }, [setDisabled, setSetupError, setup, currentStep, missingCommPrefs]);

    if (error || loading) {
        return null;
    }

    if (setupData && setupData.access_key_id) {
        return (
            <Layout logged={false}>
                <SetupComplete
                    accessKeyId={setupData.access_key_id}
                    secretAccessKey={setupData.secret_access_key}
                    apiEndpoint={API_ENDPOINT}
                    />
            </Layout>
        );
    }

    const notInitialized = currentStep !== SETUP_STATE_INITIALIZED;
    if (notInitialized || missingCommPrefs) {
        return (
            <Layout logged={false}>
                <UserConfiguration
                    onSubmit={onSubmitUserConfiguration}
                    setupError={setupError}
                    disabled={disabled}
                    requireAdmin={notInitialized}
                />
            </Layout>
        );
    }

    return router.push({pathname: '/', query: router.query});
};


const SetupPage = () => <SetupContents/>;

export default SetupPage;
