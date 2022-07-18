import {useAPI} from "../../../lib/hooks/api";
import {templates} from "../../../lib/api";
import {Loading} from "../../../lib/components/controls";
import {Box, Tab, Tabs} from "@mui/material";
import React, {useState} from "react";
import {TabPanel} from "../../../lib/components/tab_utils";

const SPARK_SUBMIT_TEMPLATE_NAME = 'spark.conf.tt';
const SPARK_EMR_TEMPLATE_NAME = 'spark.conf.tt';
const SPARK_DATABRICKS_TEMPLATE_NAME = 'spark.conf.tt';

export const SparkConfigStep = () => {
    const [confIndex, setConfIndex] = useState(0);
    const {loading, error, response} = useAPI(async () => {
        const lakeFSURLProp = {lakefs_url: window.location.origin};
        const sparkSubmitConfig = await templates.expandTemplate(SPARK_SUBMIT_TEMPLATE_NAME, lakeFSURLProp);
        const sparkEmrConfig = await templates.expandTemplate(SPARK_EMR_TEMPLATE_NAME, lakeFSURLProp);
        const sparkDBConfig = await templates.expandTemplate(SPARK_DATABRICKS_TEMPLATE_NAME, lakeFSURLProp);

        return [
            {conf: sparkSubmitConfig, title: 'spark-submit'},
            {conf: sparkEmrConfig, title: 'EMR'},
            {conf: sparkDBConfig, title: 'Databricks'}
        ]
    });

    if (error) {
       console.log(error);
    }
    if (loading) {
        return  <Loading />;
    }

    const tabs = response.map((confObj, index) => {
        return {
            tab: <Tab key={index} label={confObj.title} />,
            tabPanel: <TabPanel key={index} value={confIndex} index={index}>{confObj.conf}</TabPanel>
        }
    });
    const handleChange = (_, newConf) => {
        setConfIndex(newConf);
    }
    return (
        // error ?
        <>
            <Box sx={{width: '100%'}}>
                <Tabs
                    value={confIndex}
                    onChange={handleChange}
                    textColor="primary"
                    indicatorColor="primary"
                    aria-label="spark configurations"
                    centered
                >
                    {tabs.map((tabObj) => tabObj.tab)}
                </Tabs>
            </Box>
            {tabs.map((tabObj) => tabObj.tabPanel)}
        </>
    );
}
