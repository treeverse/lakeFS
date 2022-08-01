import {useAPI} from "../../../lib/hooks/api";
import {commits, objects, templates} from "../../../lib/api";
import {Error, ProgressSpinner} from "../../../lib/components/controls";
import {Box, Tab} from "@mui/material";
import React, {useState} from "react";
import {TabsWrapper} from "../../../lib/components/nav";
import {CodeTabPanel} from "../../../lib/components/code_tabs";
import Alert from "react-bootstrap/Alert";

const SPARK_SUBMIT_TEMPLATE_NAME = 'spark.submit.conf.tt';
const SPARK_CORE_SITE_TEMPLATE_NAME = 'spark.core.site.conf.tt';
const SPARK_DATABRICKS_TEMPLATE_NAME = 'spark.databricks.conf.tt';
const lakeFSURLProp = {lakefs_url: window.location.origin};

async function uploadAndCommitReadme(repoId, branchName, importLocation) {
    const README_TEMPLATE_NAME = 'spark.metastore.readme.tt';
    const readmeProp = {repo: repoId, branch: branchName};
    if (importLocation) {
        readmeProp['import_location'] = importLocation;
    }
    const sparkSubmitConfig = await templates.expandTemplate(README_TEMPLATE_NAME, readmeProp);
    const readmeFile = new File([sparkSubmitConfig], 'README.md', {type: 'text/markdown',});
    await objects.upload(repoId, branchName, 'README.md', readmeFile);
    await commits.commit(repoId, branchName, 'Add Spark quickstart README', {user: 'Spark quickstart'},);
}

export const SparkConfigStep = ({onComplete=()=>{}, repoId, branchName, importLocation }) => {
    const [selectedIndex, setSelectedIndex] = useState(0);
    const {loading, error, response} = useAPI(async () => {
        const sparkSubmitConfig = templates.expandTemplate(SPARK_SUBMIT_TEMPLATE_NAME, lakeFSURLProp);
        const sparkCoreSiteConfig = templates.expandTemplate(SPARK_CORE_SITE_TEMPLATE_NAME, lakeFSURLProp);
        const sparkDBConfig = templates.expandTemplate(SPARK_DATABRICKS_TEMPLATE_NAME, lakeFSURLProp);
        const readmeGeneration = uploadAndCommitReadme(repoId, branchName, importLocation);
        await Promise.all([sparkSubmitConfig, sparkCoreSiteConfig, sparkDBConfig, readmeGeneration]);
        onComplete();
        return [
            {conf: await sparkSubmitConfig, title: 'spark-submit', language: 'bash'},
            {conf: await sparkCoreSiteConfig, title: 'core-site.xml', language: 'xml'},
            {conf: await sparkDBConfig, title: 'Databricks', language: 'plaintext'}
        ]
    });

    if (error) {
        return <Error error={error}/>;
    }
    if (loading) {
        return <ProgressSpinner />;
    }

    const tabs = response.map((confObj, tabIndex) => {
        return {
            tab: <Tab key={tabIndex} label={confObj.title}/>,
            tabPanel: <CodeTabPanel key={tabIndex} isSelected={selectedIndex===tabIndex} index={tabIndex} language={confObj.language}>{confObj.conf}</CodeTabPanel>
        }
    });
    const handleChange = (_, newConf) => {
        setSelectedIndex(newConf);
    }
    return (
        <Box sx={{display: 'flex', flexDirection: 'column', justifyContent: 'space-between'}}>
            <Box sx={{width: '100%'}}>
                <Alert variant="warning" className="mt-3">Copy the credentials and store them somewhere safe. You will not be able to access them again.</Alert>
            </Box>
            <Box sx={{width: '100%'}}>
                <TabsWrapper defaultTabIndex={selectedIndex} handleTabChange={handleChange}
                             ariaLabel='spark-configurations'>
                    {tabs.map((tabObj) => tabObj.tab)}
                </TabsWrapper>
            </Box>
            <Box sx={{mt: 1}}>
                {tabs.map((tabObj) => tabObj.tabPanel)}
            </Box>
        </Box>
    );
}
