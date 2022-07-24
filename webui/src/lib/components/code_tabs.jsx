import {Box} from "@mui/material";
import {ClipboardButton} from "./controls";
import {FaRegCopy} from "react-icons/fa";
import React from "react";

export function CodeTabPanel({children, isSelected, index, ...other}) {
    return (
        <div
            role="code-tabpanel"
            hidden={!isSelected}
            id={`code-tabpanel-${index}`}
            aria-labelledby={`code-tabpanel-${index}`}
            {...other}
        >
            {isSelected && (
                <Box sx={{display: 'flex', flexDirection: 'row', justifyContent: 'space-between', py: 1}}
                     className={'code-container text-secondary'}>
                    <Box sx={{ml: 2}}>
                        {children}
                    </Box>
                    <Box sx={{mr: 2}}>
                        <ClipboardButton icon={<FaRegCopy size={16}/>} variant="link" tooltip="Copy to clipboard"
                                         text={children} size={'sm'}/>
                    </Box>
                </Box>
            )}
        </div>
    );
}
