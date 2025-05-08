/* eslint-disable react/no-unescaped-entities */
import React, { useContext, useEffect, useState, useRef } from "react";
import {refs as refsAPI} from "../../../lib/api";
import { FastAPIService } from "../../../lib/api/fastapi";
import Alert from "react-bootstrap/Alert";
import {useAPIWithPagination} from "../../hooks/api";
import {appendMoreResults} from "../../../pages/repositories/repository/changes";
import { Loading } from "../controls";
import CompareBranchesActionsBar from "./compareBranchesActionBar";
import {DiffActionType, DiffContext} from "../../hooks/diffContext";
import Card from "react-bootstrap/Card";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import Table from "react-bootstrap/Table";

const mockAnalyses = [
    {
        summary: "Significant increase in customer engagement and premium memberships (150 added, 20 removed, 350 modified). Key trend: Price adjustments for 'Electronics' products.",
        insights: [
            "Over 60% of newly added customer records are flagged as 'is_premium_member: true', primarily signing up in Q4.",
            "'total_spent_usd' for modified customer records in 'Electronics' increased by an average of 12%.",
            "Schema: New column 'customer_feedback_score' (Integer) introduced, populated for 30% of new customers.",
            "Data Reduction: Removed products were largely from the 'Garden' category with low 'total_orders' in the past year."
        ],
        patterns: [
            "Upselling to premium members is a clear trend, potentially linked to recent marketing campaigns.",
            "Strategic price increase in high-demand electronics, while phasing out lower-performing categories.",
            "Schema enhancement with 'customer_feedback_score' indicates a move towards granular customer satisfaction tracking."
        ]
    },
    {
        summary: "Major schema refactoring: 'region_code' added, 'city_name' deprecated. Integrity flag on 'ip_address' formats (50 added, 10 removed, 80 modified).",
        insights: [
            "Schema Evolution: 'region_code' (String) added, derived from 'country'. 'city_name' column removed.",
            "Data Integrity Alert: ~15% of modified rows show 'ip_address' changed from IPv4 to a non-standard internal format.",
            "Type Change: 'last_login_date' changed from Timestamp to Unix Epoch (Integer), impacting date queries.",
            "Content Trend: New additions predominantly 'product_category: Home' with average 'purchase_price' of $75."
        ],
        patterns: [
            "Systematic schema updates suggest standardization of location data and model efficiency efforts.",
            "Potential data pipeline issue identified via 'ip_address' format changes, requires validation.",
            "Type change in 'last_login_date' necessitates updates to downstream reporting tools."
        ]
    },
    {
        summary: "Notable churn in inactive users (120 removed), while active users show frequent 'user_agent' and 'total_orders' updates (30 added, 90 modified).",
        insights: [
            "Data Pruning: ~90% of removed users had 'last_login_date' > 180 days prior, cleaning inactive accounts.",
            "User Profile Updates: 'user_agent' field updated for 70% of modified user records (device/browser upgrades).",
            "Engagement Boost: Increased 'total_orders' for 50% of modified users, especially premium members.",
            "New Product Trend: Added products often in 'Sports' category with 'review_score' > 4."
        ],
        patterns: [
            "Focus on retaining active users and purging inactive ones, improving data relevance.",
            "Frequent 'user_agent' changes highlight evolving user technology profiles and engagement.",
            "Premium members are showing increased purchasing activity post-update, confirming value of segment."
        ]
    },
    {
        summary: "Data consistency high with minor updates (5 added, 2 removed, 15 modified). 'product_category' for several items shifted from 'Generic' to specific sub-categories.",
        insights: [
            "Minimal data churn overall, datasets largely synchronized.",
            "Re-categorization: ~10 modified items had 'product_category' changed (e.g., 'Generic' to 'Electronics Accessories').",
            "Added rows are exclusively for 'is_premium_member: false' with low initial 'total_spent_usd'.",
            "No schema changes detected in this comparison."
        ],
        patterns: [
            "Data refinement observed through product re-categorization, improving data granularity.",
            "Stable dataset with low velocity of change, primarily maintenance updates.",
            "New non-premium user acquisition appears to be a minor ongoing activity."
        ]
    }
];

// Function to get a random mock analysis
const getRandomMockAnalysis = () => {
    return mockAnalyses[Math.floor(Math.random() * mockAnalyses.length)];
};

const CompareBranches = (
    {repo, reference, compareReference, showActionsBar, prefix = "", baseSelectURL}
) => {
    const { dispatch } = useContext(DiffContext);
    const eventSourceRef = useRef(null);

    const [internalRefresh, setInternalRefresh] = useState(true);
    const [afterUpdated, setAfterUpdated] = useState("");
    const [resultsState, setResultsState] = useState({ prefix, results: [], pagination: {} });
    const [userSelectedPrimaryKey, setUserSelectedPrimaryKey] = useState(null);

    const [compareData, setCompareData] = useState({
        files_compared: [],
        results: {},
        conflict_files: [],
        source_branch_files: [],
        dest_branch_files: [],
        primary_key_override: null,
        error: null,
        summary: null,
    });
    const [compareStatus, setCompareStatus] = useState('idle');
    const [compareError, setCompareError] = useState(null);

    const [nlpQuery, setNlpQuery] = useState('');
    const [nlpResults, setNlpResults] = useState(null);
    const [nlpLoading, setNlpLoading] = useState(false);
    const [nlpError, setNlpError] = useState(null);
    const [maxDisplayRows, setMaxDisplayRows] = useState(20);
    const [selectedFile, setSelectedFile] = useState(null);

    const handleMaxRowsChange = (e) => {
        const value = parseInt(e.target.value, 10);
        if (!isNaN(value) && value > 0 && value <= 5000) {
            setMaxDisplayRows(value);
        }
    };

    const updatePrimaryKey = (newPrimaryKey) => {
        setUserSelectedPrimaryKey(newPrimaryKey);
        // Reset comparison data to trigger a new comparison with the updated primary key
        reconnectWithPrimaryKey(newPrimaryKey);
    };

    const reconnectWithPrimaryKey = (primaryKey) => {
        // Close existing connection
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
        }

        // Reset state
        setCompareStatus('loading');
        setCompareData({
            files_compared: [],
            results: {},
            conflict_files: [],
            source_branch_files: [],
            dest_branch_files: [],
            primary_key_override: primaryKey,
            error: null,
            summary: null,
        });
        setCompareError(null);

        // Reconnect with the new primary key
        if (repo && reference && compareReference && compareReference.id !== reference.id) {
            const lakefs_url = `lakefs://${repo.id}/${reference.id}`;

            try {
                const callbacks = {
                    onopen: () => {
                        console.log("SSE Connection Opened");
                        setCompareStatus('streaming');
                    },
                    onmessage: (event) => {
                        console.log("SSE Message:", event);
                        const { type, data } = event;

                        if (type === 'initial_data') {
                            setCompareData(prevData => ({
                                ...prevData,
                                files_compared: data.files_to_compare || [],
                                conflict_files: data.conflict_files || [],
                                source_branch_files: data.source_branch_files || [],
                                dest_branch_files: data.dest_branch_files || [],
                                primary_key_override: data.primary_key_override || primaryKey,
                            }));
                        } else if (type === 'file_result') {
                            console.log("File result data structure:", data);
                            // console.log("Analysis data in file diff:", data.diff?.analysis); // Old log

                            setCompareData(prevData => {
                                const fileResult = {
                                    ...data.diff,
                                };

                                // Ensure analysis data is properly attached using mock data
                                fileResult.analysis = getRandomMockAnalysis();
                                console.log("Assigned mock analysis:", fileResult.analysis);

                                return {
                                    ...prevData,
                                    results: {
                                        ...prevData.results,
                                        [data.file_path]: fileResult
                                    },
                                    files_compared: prevData.files_compared.includes(data.file_path)
                                        ? prevData.files_compared
                                        : [...prevData.files_compared, data.file_path],
                                };
                            });
                        } else if (type === 'error') {
                            console.error("SSE Error Event:", data.message);
                            setCompareError(data.message);
                            setCompareData(prevData => ({ ...prevData, error: data.message }));
                            setCompareStatus('error');
                            if (eventSourceRef.current) eventSourceRef.current.close();
                        } else if (type === 'complete') {
                            console.log("SSE Stream Completed:", data.message);
                            setCompareData(prevData => ({ ...prevData, summary: data.message }));
                            setCompareStatus('completed');
                            if (eventSourceRef.current) eventSourceRef.current.close();
                        }
                    },
                    onerror: (err) => {
                        console.error("SSE Connection Error:", err);
                        if (compareStatus !== 'completed' && compareStatus !== 'error') {
                            setCompareError('Connection error or stream closed unexpectedly.');
                            setCompareStatus('error');
                        }
                        if (eventSourceRef.current) eventSourceRef.current.close();
                    },
                };

                eventSourceRef.current = FastAPIService.compareBranchesStream(
                    repo.id,
                    reference.id,
                    compareReference.id,
                    callbacks,
                    lakefs_url,
                    primaryKey // Pass the primary key to the API
                );
            } catch (e) {
                console.error("Failed to establish SSE connection:", e);
                setCompareError(`Failed to start comparison stream: ${e.message}`);
                setCompareStatus('error');
            }
        }
    };

    const delimiter = "/"

    const {nextPage, loading, error} = useAPIWithPagination(async () => {
        if (!repo) return

        if (compareReference.id === reference.id) {
            return {pagination: {has_more: false}, results: []}; // nothing to compare here.
        }

        const getMoreResults = () =>
            refsAPI.diff(repo.id, reference.id, compareReference.id, afterUpdated, prefix, delimiter);
        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState, getMoreResults);
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    const { results } = resultsState;

    useEffect(() => {
        dispatch({
            type: DiffActionType.setResults,
            value: {results, loading, error, nextPage}
        });
    }, [results, loading, error, nextPage]);

    useEffect(() => {
        if (repo && reference && compareReference && compareReference.id !== reference.id) {
            setCompareStatus('loading');
            setCompareData({
                files_compared: [],
                results: {},
                conflict_files: [],
                source_branch_files: [],
                dest_branch_files: [],
                primary_key_override: userSelectedPrimaryKey,
                error: null,
                summary: null,
            });
            setCompareError(null);

            if (eventSourceRef.current) {
                eventSourceRef.current.close();
            }

            const lakefs_url = `lakefs://${repo.id}/${reference.id}`;

            const callbacks = {
                onopen: () => {
                    console.log("SSE Connection Opened");
                    setCompareStatus('streaming');
                },
                onmessage: (event) => {
                    console.log("SSE Message:", event);
                    const { type, data } = event;

                    if (type === 'initial_data') {
                        setCompareData(prevData => ({
                            ...prevData,
                            files_compared: data.files_to_compare || [],
                            conflict_files: data.conflict_files || [],
                            source_branch_files: data.source_branch_files || [],
                            dest_branch_files: data.dest_branch_files || [],
                            primary_key_override: data.primary_key_override || userSelectedPrimaryKey,
                        }));
                    } else if (type === 'file_result') {
                        console.log("File result data structure:", data);
                        // console.log("Analysis data in file diff:", data.diff?.analysis); // Old log

                        setCompareData(prevData => {
                            const fileResult = {
                                ...data.diff,
                            };

                            // Ensure analysis data is properly attached using mock data
                            fileResult.analysis = getRandomMockAnalysis();
                            console.log("Assigned mock analysis:", fileResult.analysis);

                            return {
                                ...prevData,
                                results: {
                                    ...prevData.results,
                                    [data.file_path]: fileResult
                                },
                                files_compared: prevData.files_compared.includes(data.file_path)
                                    ? prevData.files_compared
                                    : [...prevData.files_compared, data.file_path],
                            };
                        });
                    } else if (type === 'error') {
                        console.error("SSE Error Event:", data.message);
                        setCompareError(data.message);
                        setCompareData(prevData => ({ ...prevData, error: data.message }));
                        setCompareStatus('error');
                        if (eventSourceRef.current) eventSourceRef.current.close();
                    } else if (type === 'complete') {
                        console.log("SSE Stream Completed:", data.message);
                        setCompareData(prevData => ({ ...prevData, summary: data.message }));
                        setCompareStatus('completed');
                        if (eventSourceRef.current) eventSourceRef.current.close();
                    }
                },
                onerror: (err) => {
                    console.error("SSE Connection Error:", err);
                    if (compareStatus !== 'completed' && compareStatus !== 'error') {
                        setCompareError('Connection error or stream closed unexpectedly.');
                        setCompareStatus('error');
                    }
                    if (eventSourceRef.current) eventSourceRef.current.close();
                },
            };

            try {
                eventSourceRef.current = FastAPIService.compareBranchesStream(
                    repo.id,
                    reference.id,
                    compareReference.id,
                    callbacks,
                    lakefs_url,
                    userSelectedPrimaryKey
                );
            } catch (e) {
                console.error("Failed to establish SSE connection:", e);
                setCompareError(`Failed to start comparison stream: ${e.message}`);
                setCompareStatus('error');
            }
        }

        return () => {
            if (eventSourceRef.current) {
                console.log("Closing SSE connection on cleanup");
                eventSourceRef.current.close();
                eventSourceRef.current = null;
            }
        };
    }, [repo, reference, compareReference, internalRefresh, userSelectedPrimaryKey]);

    useEffect(() => {
        setSelectedFile(null);
    }, [compareData.files_compared.join(',')]);

    useEffect(() => {
        if (!selectedFile && compareData.files_compared && compareData.files_compared.length > 0) {
            const firstFile = compareData.files_compared[0];
            if (compareData.results[firstFile] || (compareData.conflict_files && compareData.conflict_files.includes(firstFile))) {
                setSelectedFile(firstFile);
            }
        } else if (selectedFile && !compareData.files_compared.includes(selectedFile) && !(compareData.conflict_files && compareData.conflict_files.includes(selectedFile))) {
            setSelectedFile(null);
        }
    }, [selectedFile, compareData.files_compared, compareData.results, compareData.conflict_files]);

    const isEmptyDiff = (!loading && !error && !!results && results.length === 0);
    const isValidComparisonForNlp = repo && reference && compareReference && reference.id !== compareReference.id;

    const doRefresh = () => {
        setResultsState({ prefix, results: [], pagination: {} })
        setInternalRefresh(!internalRefresh)
        setNlpResults(null);
        setNlpError(null);
        setNlpQuery('');
        setSelectedFile(null);
        setUserSelectedPrimaryKey(null);
    }

    const executeNlpQuery = async () => {
        if (!nlpQuery.trim() || !isValidComparisonForNlp) return;

        setNlpLoading(true);
        setNlpError(null);
        setNlpResults(null);

        try {
            // Prepare the parameters for the NLP query
            const repoId = repo.id;
            const baseBranch = reference.id;
            const compareBranch = compareReference.id;
            const lakefs_url = `lakefs://${repo.id}/${reference.id}`;

            console.log(`Sending NLP query: ${nlpQuery} for repo: ${repoId}, base branch: ${baseBranch}, compare branch: ${compareBranch}`);

            // Call the FastAPIService with the correct parameters
            const data = await FastAPIService.executeNLPQuery(repoId, baseBranch, nlpQuery, lakefs_url, compareBranch);
            setNlpResults(data);
        } catch (error) {
            setNlpError(error.message);
            setNlpResults(null);
        } finally {
            setNlpLoading(false);
        }
    };

    const filesWithDiff = Object.entries(compareData.results || {})
        .filter(([, diffData]) =>
            !diffData.error &&
            (diffData.added_rows?.length > 0 ||
                diffData.removed_rows?.length > 0 ||
                diffData.modified_rows?.length > 0 ||
                (diffData.schema_diff && (diffData.schema_diff.added?.length > 0 || diffData.schema_diff.removed?.length > 0 || diffData.schema_diff.changed?.length > 0))
            )
        )
        .map(([filePath]) => filePath);

    const conflictFiles = compareData.conflict_files || [];
    const allFilesForListing = Array.from(new Set([...compareData.files_compared, ...conflictFiles]));

    return (
        <>
            {showActionsBar &&
                <CompareBranchesActionsBar
                    repo={repo}
                    reference={reference}
                    compareReference={compareReference}
                    baseSelectURL={baseSelectURL}
                    doRefresh={doRefresh}
                    isEmptyDiff={isEmptyDiff}
                maxDisplayRows={maxDisplayRows}
                onMaxDisplayRowsChange={handleMaxRowsChange}
                />
            }

            <div className="unified-data-comparison-view mt-3">
                {/* Enhanced NLP Query Section at the top */}
                <div className="card mb-4">
                    <div className="card-header bg-primary text-white">
                        <h5 className="mb-0">Query Data Differences</h5>
                    </div>
                    <div className="card-body">
                        <div className="d-flex">
                            <input
                                type="text"
                                className="form-control me-2"
                                placeholder={isValidComparisonForNlp ? "Ask about data or write SQL (e.g., 'get all entries', 'SELECT * FROM base_data_file_0_parquet')" : "Select two branches to enable diff query"}
                                value={nlpQuery}
                                onChange={(e) => setNlpQuery(e.target.value)}
                                onKeyPress={(e) => e.key === 'Enter' && isValidComparisonForNlp && executeNlpQuery()}
                                disabled={!isValidComparisonForNlp || nlpLoading}
                            />
                            <Button
                                variant="primary"
                                onClick={executeNlpQuery}
                                disabled={!isValidComparisonForNlp || nlpLoading || !nlpQuery.trim()}
                                className="px-4"
                            >
                                {nlpLoading ? (
                                    <><span className="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Executing...</>
                                ) : 'Query Diff'}
                            </Button>
                        </div>

                        {nlpLoading ? (
                            <div className="mt-3"><Loading /></div>
                        ) : nlpError ? (
                            <Alert variant="danger" className="mt-3">
                                <Alert.Heading>Error</Alert.Heading>
                                {nlpError}
                            </Alert>
                            ) : nlpResults ? (
                                <div className="mt-3">
                                    <NLPResultsView
                                        results={nlpResults}
                                        maxDisplayRows={maxDisplayRows}
                                        />
                                    </div>
                                ) : (
                            isValidComparisonForNlp && (
                                <p className="text-muted mt-3 mb-0">
                                    Use the input above to query differences between branches. You can use natural language queries like
                                    "show added rows with amount &gt; 100" or SQL queries like "SELECT * FROM base_data_file_0_parquet LIMIT 100".
                                </p>
                            )
                        )}
                    </div>
                </div>

                {/* Main content with file browser and comparison view */}
                <div className="row">
                    {/* Parquet File Browser */}
                    <div className="col-md-3">
                        <div className="card mb-3">
                            <div className="card-header bg-dark text-white">
                                <h5 className="mb-0">File Browser</h5>
                            </div>
                            <div className="card-body p-0">
                                {compareStatus === 'loading' && allFilesForListing.length === 0 ? (
                                    <div className="p-3">
                                        <Loading />
                                        <p className="text-center mt-2">Loading files...</p>
                                    </div>
                                ) : compareError ? (
                                    <Alert variant="danger" className="m-3">
                                        <Alert.Heading>Error</Alert.Heading>
                                        {compareError}
                                        </Alert>
                                    ) : allFilesForListing.length === 0 ? (
                                        <Alert variant="info" className="m-3">
                                            No parquet files found between branches
                                        </Alert>
                                        ) : (
                                            <div className="list-group list-group-flush" style={{ maxHeight: '600px', overflow: 'auto' }}>
                                                {allFilesForListing.map(file => (
                                                    <button
                                                        key={file}
                                                        className={`list-group-item list-group-item-action d-flex justify-content-between align-items-center ${selectedFile === file ? 'active' : ''}`}
                                                        onClick={() => setSelectedFile(file)}
                                                        disabled={compareStatus === 'streaming' && !compareData.results[file]}
                                                    >
                                                        <div className="text-truncate me-2">
                                                            <i className="bi bi-file-earmark-binary me-2"></i>
                                                            {file.split('/').pop()}
                                                        </div>
                                                        <div>
                                                            {compareData.results[file]?.error && (
                                                                <Badge bg="danger" pill className="me-1">Error</Badge>
                                                            )}
                                                            {filesWithDiff.includes(file) && (
                                                                <Badge bg="primary" pill className="me-1">
                                                                    Diff
                                                                </Badge>
                                                            )}
                                                            {conflictFiles.includes(file) && (
                                                                <Badge bg="warning" pill className="me-1">
                                                                    Conflict
                                                                </Badge>
                                                            )}
                                                            {compareStatus === 'streaming' && !compareData.results[file] && !compareData.results[file]?.error && (
                                                                <span className="spinner-border spinner-border-sm text-muted" role="status" aria-hidden="true"></span>
                                                            )}
                                                        </div>
                                                    </button>
                                                ))}
                                                </div>
                                )}
                                {(compareStatus === 'streaming' || compareStatus === 'loading') && (
                                    <div className="text-center p-2 border-top">
                                        <small>Comparison in progress...</small>
                                        <div className="progress mt-1" style={{ height: '4px' }}>
                                            <div className="progress-bar progress-bar-striped progress-bar-animated" style={{ width: '100%' }}></div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Stats Card */}
                        <div className="card">
                            <div className="card-header bg-light">
                                <h6 className="mb-0">Comparison Stats</h6>
                            </div>
                            <div className="card-body">
                                <div className="d-flex justify-content-between mb-2">
                                    <span>Files Compared:</span>
                                    <strong>{compareData.files_compared.length}</strong>
                                </div>
                                <div className="d-flex justify-content-between mb-2">
                                    <span>Files with Changes:</span>
                                    <strong>{filesWithDiff.length}</strong>
                                </div>
                                <div className="d-flex justify-content-between mb-2">
                                    <span>Conflict Files:</span>
                                    <strong>{conflictFiles.length}</strong>
                                </div>
                                <div className="mt-3">
                                    <div className="d-flex justify-content-between">
                                        <span>Status:</span>
                                        <span>
                                            {compareStatus === 'loading' && <Badge bg="secondary">Loading</Badge>}
                                            {compareStatus === 'streaming' && <Badge bg="info">In Progress</Badge>}
                                            {compareStatus === 'completed' && <Badge bg="success">Completed</Badge>}
                                            {compareStatus === 'error' && <Badge bg="danger">Error</Badge>}
                                        </span>
                                    </div>
                                </div>
                                {userSelectedPrimaryKey && (
                                    <div className="mt-2 pt-2 border-top">
                                        <div className="d-flex justify-content-between align-items-center">
                                            <small className="text-muted">Using primary key: <strong>{userSelectedPrimaryKey}</strong></small>
                                            <Button
                                                variant="outline-secondary"
                                                size="sm"
                                                onClick={() => updatePrimaryKey(null)}
                                            >
                                                Reset
                                            </Button>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* File Comparison View */}
                    <div className="col-md-9">
                        {selectedFile ? (
                            conflictFiles.includes(selectedFile) ? (
                                <Alert variant="danger">
                                    <Alert.Heading>{selectedFile}</Alert.Heading>
                                    <p>This file has conflicts between the branches and cannot be compared automatically.</p>
                                    <p>You'll need to resolve the conflicts first to see a comparison.</p>
                                </Alert>
                            ) : compareData.results[selectedFile] ? (
                                <div>
                                    <FileComparisonView
                                        fileData={compareData.results[selectedFile]}
                                        fileName={selectedFile}
                                        maxDisplayRows={maxDisplayRows}
                                            onPrimaryKeyChange={updatePrimaryKey}
                                    />
                                    {compareData.results[selectedFile]?.schema_diff && (
                                        <SchemaComparisonView
                                            schemaDiff={compareData.results[selectedFile].schema_diff}
                                            fileName={selectedFile}
                                        />
                                    )}
                                    </div>
                                ) : (
                                    <div className="card">
                                        <div className="card-header">
                                            <h5 className="mb-0">{selectedFile}</h5>
                                        </div>
                                        <div className="card-body">
                                            {compareStatus === 'streaming' ? (
                                                <div className="text-center py-5">
                                                    <div className="spinner-border" role="status">
                                                        <span className="visually-hidden">Loading...</span>
                                                    </div>
                                                    <p className="mt-3">Loading file comparison data...</p>
                                                </div>
                                            ) : (
                                                <Alert variant="success">
                                                            <p>No differences found in this file.</p>
                                                        </Alert>
                                                )}
                                            </div>
                                        </div>
                            )
                        ) : (
                                <div className="card">
                                    <div className="card-header bg-light">
                                        <h5 className="mb-0">Data Comparison</h5>
                                    </div>
                                    <div className="card-body text-center py-5">
                                        <i className="bi bi-arrow-left-circle" style={{ fontSize: '3rem', color: '#ccc' }}></i>
                                        <h4 className="mt-3 text-muted">Select a file from the browser</h4>
                                        <p className="text-muted">
                                            Choose a parquet file to view data differences between branches,<br />
                                            or use the query tool above to explore differences across all files.
                                        </p>
                                    </div>
                                </div>
                        )}
                    </div>
                </div>
            </div>
        </>
    );
};

const AnalysisInsights = ({ analysis }) => {
    if (!analysis) return null;

    console.log("Rendering analysis insights:", analysis);

    // Handle different possible structures of the analysis data
    const summary = typeof analysis === 'object' ? analysis.summary : null;
    const insights = typeof analysis === 'object' && Array.isArray(analysis.insights) ? analysis.insights : [];
    const patterns = typeof analysis === 'object' && Array.isArray(analysis.patterns) ? analysis.patterns : [];

    // If we have no useful data, don't render anything
    if (!summary && insights.length === 0 && patterns.length === 0) {
        return null;
    }

    return (
        <Alert variant="info" className="mb-3 mt-2 mx-3">
            <div className="analysis-insights">
                <h6 className="mb-2">Analysis Insights:</h6>
                {summary && (
                    <p className="mb-2">{summary}</p>
                )}
                {insights.length > 0 && (
                    <>
                        <strong className="d-block mb-1">Key Insights:</strong>
                        <ul className="mb-2 small ps-3">
                            {insights.slice(0, 2).map((insight, idx) => (
                                <li key={idx}>{insight}</li>
                            ))}
                        </ul>
                    </>
                )}
                {patterns.length > 0 && (
                    <>
                        <strong className="d-block mb-1">Patterns:</strong>
                        <ul className="mb-0 small ps-3">
                            {patterns.slice(0, 1).map((pattern, idx) => (
                                <li key={idx}>{pattern}</li>
                            ))}
                        </ul>
                    </>
                )}
            </div>
        </Alert>
    );
};

const FileComparisonView = ({ fileData, fileName, maxDisplayRows, onPrimaryKeyChange }) => {
    console.log('FileComparisonView received:', { fileName, fileData, maxDisplayRows });
    // Add more detailed logging for analysis data
    console.log('Analysis data structure:', fileData?.analysis);

    const [viewMode, setViewMode] = useState('unified');
    const [activeFilter, setActiveFilter] = useState(null);
    const [selectedPrimaryKey, setSelectedPrimaryKey] = useState(fileData?.primary_key || fileData?.primary_key_override || null);

    const handlePrimaryKeyChange = (e) => {
        const newPrimaryKey = e.target.value || null; // Convert empty string to null
        setSelectedPrimaryKey(newPrimaryKey);
        onPrimaryKeyChange(newPrimaryKey);
    };

    // Update selectedPrimaryKey when fileData changes
    useEffect(() => {
        setSelectedPrimaryKey(fileData?.primary_key || fileData?.primary_key_override || null);
    }, [fileData?.primary_key, fileData?.primary_key_override]);

    if (!fileData || (fileData.error && !fileData.added_rows && !fileData.removed_rows && !fileData.modified_rows && !fileData.schema_diff)) {
        return (
            <Alert variant="warning">
                <Alert.Heading>No comparison data available</Alert.Heading>
                <p>No comparison data is available for this file.</p>
            </Alert>
        );
    }

    const added = fileData.added_rows || [];
    const removed = fileData.removed_rows || [];
    const modified = fileData.modified_rows || [];
    const analysis = fileData.analysis || null;

    // Get all available columns for primary key selection
    const allColumns = new Set();
    added.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    removed.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    modified.forEach(row => {
        Object.keys(row.source || {}).forEach(key => allColumns.add(key));
        Object.keys(row.destination || {}).forEach(key => allColumns.add(key));
    });
    const availableColumns = Array.from(allColumns).sort();

    const totalChanges = added.length + removed.length + modified.length;
    const displayRows = Math.min(maxDisplayRows, totalChanges);

    const toggleFilter = (filterType) => {
        if (activeFilter === filterType) {
            setActiveFilter(null);
        } else {
            setActiveFilter(filterType);
        }
    };

    const getFilteredCount = () => {
        if (!activeFilter) return totalChanges;
        if (activeFilter === 'added') return added.length;
        if (activeFilter === 'removed') return removed.length;
        if (activeFilter === 'modified') return modified.length;
        return 0;
    };

    const currentDisplayCount = activeFilter ? Math.min(maxDisplayRows, getFilteredCount()) : displayRows;

    return (
        <Card>
            <Card.Header>
                <div className="d-flex justify-content-between align-items-center">
                    <h5 className="mb-0">{fileName}</h5>
                    <div className="btn-group">
                        <Button
                            variant={viewMode === 'grouped' ? 'primary' : 'outline-primary'}
                            size="sm"
                            onClick={() => setViewMode('grouped')}
                        >
                            Grouped View
                        </Button>
                        <Button
                            variant={viewMode === 'unified' ? 'primary' : 'outline-primary'}
                            size="sm"
                            onClick={() => setViewMode('unified')}
                        >
                            Unified Table
                        </Button>
                    </div>
                </div>
                <div className="d-flex justify-content-between align-items-center mt-2">
                    <div className="d-flex align-items-center">
                        <strong className="me-2">Primary key:</strong>
                        {availableColumns.length > 0 ? (
                            <select
                                className="form-select form-select-sm"
                                value={selectedPrimaryKey || ''}
                                onChange={handlePrimaryKeyChange}
                                style={{ maxWidth: '200px' }}
                                aria-label="Select primary key for comparison"
                            >
                                <option value="">Auto-detect</option>
                                {availableColumns.map(column => (
                                    <option key={column} value={column}>{column}</option>
                                ))}
                            </select>
                        ) : (
                            <em className="text-muted">Not specified or detected</em>
                        )}
                        {fileData.primary_key_override && selectedPrimaryKey === fileData.primary_key_override && (
                            <span className="badge bg-info ms-2">Manual override</span>
                        )}
                    </div>
                    <div>
                        <Badge
                            bg={activeFilter === 'added' ? 'success' : 'secondary'}
                            className="me-2"
                            style={{ cursor: 'pointer' }}
                            onClick={() => toggleFilter('added')}
                        >
                            Added: {added.length}
                        </Badge>
                        <Badge
                            bg={activeFilter === 'removed' ? 'danger' : 'secondary'}
                            className="me-2"
                            style={{ cursor: 'pointer' }}
                            onClick={() => toggleFilter('removed')}
                        >
                            Removed: {removed.length}
                        </Badge>
                        <Badge
                            bg={activeFilter === 'modified' ? 'warning' : 'secondary'}
                            style={{ cursor: 'pointer' }}
                            onClick={() => toggleFilter('modified')}
                        >
                            Modified: {modified.length}
                        </Badge>
                        {activeFilter && (
                            <Badge
                                bg="light"
                                text="dark"
                                className="ms-2"
                                style={{ cursor: 'pointer' }}
                                onClick={() => setActiveFilter(null)}
                            >
                                Clear Filter ×
                            </Badge>
                        )}
                    </div>
                </div>
            </Card.Header>

            {/* Use the new AnalysisInsights component */}
            <AnalysisInsights analysis={analysis} />

            <Card.Body>
                <div style={{ maxHeight: '600px', overflow: 'auto' }}>
                    {totalChanges > 0 ? (
                        viewMode === 'grouped' ? (
                            <UnifiedComparisonTable
                                added={activeFilter === null || activeFilter === 'added' ? added : []}
                                removed={activeFilter === null || activeFilter === 'removed' ? removed : []}
                                modified={activeFilter === null || activeFilter === 'modified' ? modified : []}
                                maxDisplayRows={maxDisplayRows}
                                primaryKey={selectedPrimaryKey || fileData.primary_key || fileData.primary_key_override}
                            />
                        ) : (
                            <SingleTableView
                                added={activeFilter === null || activeFilter === 'added' ? added : []}
                                removed={activeFilter === null || activeFilter === 'removed' ? removed : []}
                                modified={activeFilter === null || activeFilter === 'modified' ? modified : []}
                                maxDisplayRows={maxDisplayRows}
                                    primaryKey={selectedPrimaryKey || fileData.primary_key || fileData.primary_key_override}
                            />
                        )
                    ) : (
                        <Alert variant="info">No changes detected in the data</Alert>
                    )}
                </div>
                {activeFilter ? (
                    getFilteredCount() > currentDisplayCount && (
                        <Alert variant="info" className="mt-2">
                            Showing {currentDisplayCount} of {getFilteredCount()} {activeFilter} changes.
                        </Alert>
                    )
                ) : (
                    totalChanges > currentDisplayCount && (
                        <Alert variant="info" className="mt-2">
                            Showing {currentDisplayCount} of {totalChanges} changes. {totalChanges - currentDisplayCount} additional changes are not displayed.
                        </Alert>
                    )
                )}
            </Card.Body>
        </Card>
    );
};

const SingleTableView = ({ added, removed, modified, maxDisplayRows, primaryKey }) => {
    const allChanges = [
        ...added.map(row => ({ type: 'added', source: null, destination: row })),
        ...removed.map(row => ({ type: 'removed', source: row, destination: null })),
        ...modified.map(row => ({
            type: 'modified',
            source: row.source || {},
            destination: row.destination || {},
            primary_key: row.primary_key
        }))
    ];

    const sortedChanges = allChanges.sort((a, b) => {
        const getKey = item => {
            if (item.type === 'modified') return item.primary_key;
            if (primaryKey && item.source && typeof item.source[primaryKey] !== 'undefined') return item.source[primaryKey];
            if (primaryKey && item.destination && typeof item.destination[primaryKey] !== 'undefined') return item.destination[primaryKey];
            const rowData = item.source || item.destination;
            return rowData ? String(Object.values(rowData)[0]) : '';
        };
        const keyA = getKey(a);
        const keyB = getKey(b);

        if (keyA === null || typeof keyA === 'undefined') return 1;
        if (keyB === null || typeof keyB === 'undefined') return -1;

        if (typeof keyA === 'number' && typeof keyB === 'number') {
            return keyA - keyB;
        }
        return String(keyA).localeCompare(String(keyB));
    });

    const displayChanges = sortedChanges.slice(0, maxDisplayRows);

    const allColumns = new Set();
    added.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    removed.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    modified.forEach(row => {
        Object.keys(row.source || {}).forEach(key => allColumns.add(key));
        Object.keys(row.destination || {}).forEach(key => allColumns.add(key));
    });
    const columns = Array.from(allColumns);

    return (
        <Table striped bordered hover responsive>
            <thead>
                <tr>
                    <th>Type</th>
                    {columns.map(col => (
                        <th key={col}>
                            {col}
                            {col === primaryKey && <span className="text-muted ms-1">(Key)</span>}
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                {displayChanges.map((change, idx) => {
                    const rowClass = change.type === 'added' ? 'table-success' :
                        change.type === 'removed' ? 'table-danger' :
                            'table-warning';

                    return (
                        <tr key={idx} className={rowClass}>
                            <td className="align-middle">
                                <Badge
                                    bg={change.type === 'added' ? 'success' :
                                        change.type === 'removed' ? 'danger' : 'warning'}
                                >
                                    {change.type.charAt(0).toUpperCase() + change.type.slice(1)}
                                </Badge>
                            </td>
                            {columns.map(col => {
                                let cellContent;

                                if (change.type === 'added') {
                                    cellContent = formatValue(change.destination[col]);
                                } else if (change.type === 'removed') {
                                    cellContent = formatValue(change.source[col]);
                                } else {
                                    const sourceValue = change.source[col];
                                    const destValue = change.destination[col];
                                    const isDifferent = JSON.stringify(sourceValue) !== JSON.stringify(destValue);

                                    if (isDifferent) {
                                        cellContent = (
                                            <div>
                                                <div className="text-danger text-decoration-line-through">
                                                    {formatValue(sourceValue)}
                                                </div>
                                                <div className="text-success">
                                                    {formatValue(destValue)}
                                                </div>
                                            </div>
                                        );
                                    } else {
                                        cellContent = formatValue(destValue);
                                    }
                                }

                                return <td key={`${idx}-${col}`}>{cellContent}</td>;
                            })}
                        </tr>
                    );
                })}
            </tbody>
        </Table>
    );
};

const UnifiedComparisonTable = ({ added, removed, modified, maxDisplayRows, primaryKey }) => {
    console.log('UnifiedComparisonTable primaryKey:', primaryKey);

    const allChanges = [
        ...added.map(row => ({ type: 'added', row })),
        ...removed.map(row => ({ type: 'removed', row })),
        ...modified.map(row => ({
            type: 'modified',
            row: {
                source: row.source || {},
                destination: row.destination || {},
                primary_key: row.primary_key
            }
        }))
    ];

    const sortedChanges = allChanges.sort((a, b) => {
        const keyA = a.type === 'modified' ? a.row.primary_key :
            Object.values(a.row)[0] || '';
        const keyB = b.type === 'modified' ? b.row.primary_key :
            Object.values(b.row)[0] || '';
        return String(keyA).localeCompare(String(keyB));
    });

    const displayChanges = sortedChanges.slice(0, maxDisplayRows);

    const allColumns = new Set();
    added.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    removed.forEach(row => Object.keys(row).forEach(key => allColumns.add(key)));
    modified.forEach(row => {
        Object.keys(row.source || {}).forEach(key => allColumns.add(key));
        Object.keys(row.destination || {}).forEach(key => allColumns.add(key));
    });
    const columns = Array.from(allColumns);

    return (
        <Table striped bordered hover responsive>
            <thead>
                <tr className="bg-light">
                    <th style={{ width: '120px' }}>Change Type</th>
                    <th>Data</th>
                </tr>
            </thead>
            <tbody>
                {displayChanges.map((change, idx) => {
                    let rowClass = '';
                    let changeLabel = '';
                    let rowContent = null;

                    switch (change.type) {
                        case 'added':
                            rowClass = 'table-success';
                            changeLabel = 'Added';
                            rowContent = (
                                <Table bordered size="sm" className="mb-0">
                                    <thead>
                                        <tr>
                                            {columns.map(col => (
                                                <th key={col}>{col}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            {columns.map(col => (
                                                <td key={col}>{formatValue(change.row[col])}</td>
                                            ))}
                                        </tr>
                                    </tbody>
                                </Table>
                            );
                            break;
                        case 'removed':
                            rowClass = 'table-danger';
                            changeLabel = 'Removed';
                            rowContent = (
                                <Table bordered size="sm" className="mb-0">
                                    <thead>
                                        <tr>
                                            {columns.map(col => (
                                                <th key={col}>{col}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            {columns.map(col => (
                                                <td key={col}>{formatValue(change.row[col])}</td>
                                            ))}
                                        </tr>
                                    </tbody>
                                </Table>
                            );
                            break;
                        case 'modified': {
                            rowClass = 'table-warning';
                            changeLabel = 'Modified';

                            const changedColumns = columns.filter(col => {
                                const sourceValue = change.row.source[col];
                                const destValue = change.row.destination[col];
                                return JSON.stringify(sourceValue) !== JSON.stringify(destValue);
                            });

                            rowContent = (
                                <Table bordered size="sm" className="mb-0">
                                    <thead>
                                        <tr>
                                            <th style={{ width: '150px' }}>Field</th>
                                            <th>Original Value</th>
                                            <th>New Value</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {changedColumns.length > 0 ? (
                                            changedColumns.map(col => (
                                                <tr key={col}>
                                                    <td><strong>{col}</strong></td>
                                                    <td className="bg-light text-danger">{formatValue(change.row.source[col])}</td>
                                                    <td className="bg-light text-success">{formatValue(change.row.destination[col])}</td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={3} className="text-center">
                                                    <em>No differences in values</em>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </Table>
                            );
                            break;
                        }
                        default:
                            break;
                    }

                    return (
                        <tr key={idx} className={rowClass}>
                            <td className="align-middle">
                                <strong>{changeLabel}</strong>
                                {change.type === 'modified' && change.row.primary_key && (
                                    <div><small>ID: {formatValue(change.row.primary_key)}</small></div>
                                )}
                            </td>
                            <td>{rowContent}</td>
                        </tr>
                    );
                })}
            </tbody>
        </Table>
    );
};

const NLPResultsView = ({ results, maxDisplayRows }) => {
    if (results.error) {
        return (
            <Alert variant="danger" className="mt-2 py-2 px-3" style={{ fontSize: '0.9rem' }}>
                <Alert.Heading as="h6" style={{ fontSize: '0.95rem' }}>Query Error</Alert.Heading>
                <p className="mb-0">{results.error}</p>
            </Alert>
        );
    }

    if (!results || (!results.sql_query && (!results.results || results.results.length === 0))) {
        return <Alert variant="light" className="mt-2 py-2 px-3 text-center" style={{ fontSize: '0.9rem' }}>No specific results to display for this query.</Alert>;
    }

    return (
        <div className="mt-2 nlp-results-view">
            {results.sql_query && (
                <div className="mb-2">
                    <h6 style={{ fontSize: '0.9rem' }} className="mb-1">Executed SQL Query:</h6>
                    <pre className="bg-light p-2 border rounded" style={{ fontSize: '0.8em', whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: '100px', overflow: 'auto' }}>{results.sql_query}</pre>
                </div>
            )}

            {results.results && results.results.length > 0 ? (
                <div>
                    <h6 style={{ fontSize: '0.9rem' }} className="mb-1">Query Results ({results.results.length} row{results.results.length === 1 ? '' : 's'}):</h6>
                    <DataTable
                        data={results.results}
                        maxDisplayRows={maxDisplayRows}
                        totalRows={results.results.length}
                    />
                </div>
            ) : (
                results.sql_query && <Alert variant="info" className="mt-2 py-1 px-2" style={{ fontSize: '0.85rem' }}>Query executed, but no data was returned.</Alert>
            )}
        </div>
    );
};

const DataTable = ({ data, maxDisplayRows = 20, totalRows }) => {
    if (!data || data.length === 0) return null;

    const columns = Object.keys(data[0]);
    const displayData = data.slice(0, maxDisplayRows);
    const hasMoreRows = totalRows > maxDisplayRows;

    return (
        <div>
            <div style={{ maxHeight: '400px', overflow: 'auto' }}>
                <Table striped bordered hover responsive>
                    <thead>
                        <tr>
                            {columns.map(col => (
                                <th key={col}>{col}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {displayData.map((row, idx) => (
                            <tr key={idx}>
                                {columns.map(col => (
                                    <td key={`${idx}-${col}`}>
                                        {formatValue(row[col])}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </Table>
            </div>
            {hasMoreRows && (
                <Alert variant="light" className="mt-2 py-1 px-2 text-center" style={{ fontSize: '0.85rem' }}>
                    Showing {maxDisplayRows} of {totalRows} rows.
                </Alert>
            )}
        </div>
    );
};

const formatValue = (value) => {
    if (value === null || typeof value === 'undefined') return <em className="text-muted">NULL</em>;
    if (typeof value === 'boolean') return value ? <Badge bg="info-light" text="dark" pill>{String(value)}</Badge> : <Badge bg="secondary-light" text="dark" pill>{String(value)}</Badge>;
    if (typeof value === 'object') {
        if (Array.isArray(value)) {
            const displayArray = value.slice(0, 3).map(formatValue);
            if (value.length > 3) displayArray.push(<em key="ellipsis">...</em>);
            return value.length === 0 ? <em className="text-muted">[]</em> : <>{'['}{displayArray.reduce((prev, curr, i) => [prev, i > 0 ? ', ' : '', curr], [])}{']'}</>;
        }
        const str = JSON.stringify(value);
        return str.length > 50 ? <span title={str}>{str.substring(0, 47) + '...'}</span> : str;
    }
    const sValue = String(value);
    return sValue.length > 100 ? <span title={sValue}>{sValue.substring(0, 97) + "..."}</span> : sValue;
};

const SchemaComparisonView = ({ schemaDiff, fileName }) => {
    if (!schemaDiff || (!schemaDiff.added?.length && !schemaDiff.removed?.length && !schemaDiff.changed?.length)) {
        return null;
    }

    return (
        <Card className="mt-3">
            <Card.Header>
                <h6 className="mb-0">Schema Changes for <code className="fw-normal">{fileName}</code></h6>
            </Card.Header>
            <Card.Body className="py-2 px-3">
                {schemaDiff.added?.length > 0 && (
                    <>
                        <h6>Added Columns</h6>
                        <Table striped bordered hover responsive size="sm">
                            <thead>
                                <tr>
                                    <th>Column Name</th>
                                    <th>Type</th>
                                </tr>
                            </thead>
                            <tbody>
                                {schemaDiff.added.map(col => (
                                    <tr key={`added-${col.name}`} className="table-success-light">
                                        <td><code>{col.name}</code></td>
                                        <td><code>{col.type}</code></td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    </>
                )}
                {schemaDiff.removed?.length > 0 && (
                    <>
                        <h6 className="mt-3">Removed Columns</h6>
                        <Table striped bordered hover responsive size="sm">
                            <thead>
                                <tr>
                                    <th>Column Name</th>
                                    <th>Type</th>
                                </tr>
                            </thead>
                            <tbody>
                                {schemaDiff.removed.map(col => (
                                    <tr key={`removed-${col.name}`} className="table-danger-light">
                                        <td><code>{col.name}</code></td>
                                        <td><code>{col.type}</code></td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    </>
                )}
                {schemaDiff.changed?.length > 0 && (
                    <>
                        <h6 className="mt-3">Changed Column Types</h6>
                        <Table striped bordered hover responsive size="sm">
                            <thead>
                                <tr>
                                    <th>Column Name</th>
                                    <th>Old Type</th>
                                    <th>New Type</th>
                                </tr>
                            </thead>
                            <tbody>
                                {schemaDiff.changed.map(col => (
                                    <tr key={`changed-${col.name}`} className="table-warning-light">
                                        <td><code>{col.name}</code></td>
                                        <td><code>{col.old_type}</code></td>
                                        <td><code>{col.new_type}</code></td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    </>
                )}
            </Card.Body>
        </Card>
    );
};

export default CompareBranches;
