import {useAPI} from "../../../lib/hooks/api";
import {templates} from "../../../lib/api";
import {Error, Loading} from "../../../lib/components/controls";
import {Box, Tab} from "@mui/material";
import React, {useState} from "react";
import {TabsWrapper} from "../../../lib/components/nav";
import {CodeTabPanel} from "../../../lib/components/code_tabs";

const SPARK_SUBMIT_TEMPLATE_NAME = 'spark.submit.conf.tt';
const SPARK_CORE_SITE_TEMPLATE_NAME = 'spark.core.site.conf.tt';
const SPARK_DATABRICKS_TEMPLATE_NAME = 'spark.databricks.conf.tt';
const lakeFSURLProp = {lakefs_url: window.location.origin};

export const SparkConfigStep = ({onComplete=()=>{}}) => {
    const [selectedIndex, setSelectedIndex] = useState(0);
    const {loading, error, response} = useAPI(async () => {
        const sparkSubmitConfig = templates.expandTemplate(SPARK_SUBMIT_TEMPLATE_NAME, lakeFSURLProp);
        const sparkCoreSiteConfig = templates.expandTemplate(SPARK_CORE_SITE_TEMPLATE_NAME, lakeFSURLProp);
        const sparkDBConfig = templates.expandTemplate(SPARK_DATABRICKS_TEMPLATE_NAME, lakeFSURLProp);
        await Promise.all([sparkSubmitConfig, sparkCoreSiteConfig, sparkDBConfig]);
        onComplete();
        return [
            {conf: await sparkSubmitConfig, title: 'spark-submit'},
            {conf: await sparkCoreSiteConfig, title: 'core-site.xml'},
            {conf: await sparkDBConfig, title: 'Databricks'}
        ]
    });

    if (error) {
        return <Error error={error}/>;
    }
    if (loading) {
        return <Loading/>;
    }

    const tabs = response.map((confObj, tabIndex) => {
        return {
            tab: <Tab key={tabIndex} label={confObj.title}/>,
            tabPanel: <CodeTabPanel key={tabIndex} isSelected={selectedIndex===tabIndex} index={tabIndex}>{confObj.conf}</CodeTabPanel>
        }
    });
    const handleChange = (_, newConf) => {
        setSelectedIndex(newConf);
    }
    return (
        <Box sx={{display: 'flex', flexDirection: 'column', justifyContent: 'space-between'}}>
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
