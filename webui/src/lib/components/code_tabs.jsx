import {Box} from "@mui/material";
import {ClipboardButton} from "./controls";
import {FaRegCopy} from "react-icons/fa";
import React from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import {a11yLight} from "react-syntax-highlighter/src/styles/hljs";

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
                        <SyntaxHighlighter style={a11yLight} wrapLongLines customStyle={{margin: 0, padding: 0, backgroundColor: 'inherit'}}>
                            {children}
                        </SyntaxHighlighter>
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
