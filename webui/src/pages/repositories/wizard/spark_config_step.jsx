import {useAPI} from "../../../lib/hooks/api";
import {templates} from "../../../lib/api";
import {Error, Loading} from "../../../lib/components/controls";
import {Box, Tab} from "@mui/material";
import React, {useState} from "react";
import {CodeTabPanel, TabsWrapper} from "../../../lib/components/nav";

const SPARK_SUBMIT_TEMPLATE_NAME = 'spark.conf.tt';
const SPARK_EMR_TEMPLATE_NAME = 'spark.conf.tt';
const SPARK_DATABRICKS_TEMPLATE_NAME = 'spark.conf.tt';
const lakeFSURLProp = {lakefs_url: window.location.origin};

export const SparkConfigStep = ({onComplete=()=>{}}) => {
    const [confIndex, setConfIndex] = useState(0);
    const {loading, error, response} = useAPI(async () => {
        const sparkSubmitConfig = await templates.expandTemplate(SPARK_SUBMIT_TEMPLATE_NAME, lakeFSURLProp);
        const sparkEmrConfig = await templates.expandTemplate(SPARK_EMR_TEMPLATE_NAME, lakeFSURLProp);
        const sparkDBConfig = await templates.expandTemplate(SPARK_DATABRICKS_TEMPLATE_NAME, lakeFSURLProp);
        onComplete();
        return [
            {conf: sparkSubmitConfig, title: 'spark-submit'},
            {conf: sparkEmrConfig, title: 'EMR'},
            {conf: sparkDBConfig, title: 'Databricks'}
        ]
    });

    if (error) {
        return <Error error={error}/>;
    }
    if (loading) {
        return <Loading/>;
    }

    const tabs = response.map((confObj, index) => {
        return {
            tab: <Tab key={index} label={confObj.title}/>,
            tabPanel: <CodeTabPanel key={index} value={confIndex} index={index}>{confObj.conf}</CodeTabPanel>
        }
    });
    const handleChange = (_, newConf) => {
        setConfIndex(newConf);
    }
    return (
        error ?
            <Error error={error}/> :
            <Box sx={{display: 'flex', flexDirection: 'column', justifyContent: 'space-between'}}>
                <Box sx={{width: '100%'}}>
                    <TabsWrapper defaultTabIndex={confIndex} handleTabChange={handleChange}
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
