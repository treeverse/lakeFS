import API_CONFIG from './config';

/**
 * Service for interacting with FastAPI endpoints
 */
export const FastAPIService = {
    /**
     * Compare branches data
     * @param {string} repoId - Repository ID
     * @param {string} sourceBranch - Source branch name
     * @param {string} destBranch - Destination branch name
     * @param {string|null} lakefs_url - Optional lakefs URL in format lakefs://repo/branch
     * @param {string|null} primaryKey - Optional primary key
     * @returns {Promise<Object>} - Comparison results
     */
    compareBranches: async (repoId, sourceBranch, destBranch, lakefs_url = null, primaryKey = null) => {
        try {
            const response = await fetch(API_CONFIG.COMPARE_ENDPOINT, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    repository: repoId,
                    source_branch: sourceBranch,
                    destination_branch: destBranch,
                    lakefs_url: lakefs_url,
                    primary_key: primaryKey,
                }),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to fetch comparison data');
            }

            return await response.json();
        } catch (error) {
            console.error('Error in compareBranches:', error);
            throw error;
        }
    },

    /**
     * Compare branches data via Server-Sent Events (SSE)
     * @param {string} repoId - Repository ID
     * @param {string} sourceBranch - Source branch name
     * @param {string} destBranch - Destination branch name
     * @param {Object} callbacks - Object with onopen, onmessage, onerror, onclose callbacks
     * @param {string|null} lakefs_url - Optional lakefs URL in format lakefs://repo/branch
     * @param {string|null} primaryKey - Optional primary key
     * @returns {EventSource} - The EventSource instance
     */
    compareBranchesStream: (repoId, sourceBranch, destBranch, callbacks, lakefs_url = null, primaryKey = null) => {
        const params = new URLSearchParams({
            repository: repoId,
            source_branch: sourceBranch,
            destination_branch: destBranch,
        });
        if (lakefs_url) {
            params.append('lakefs_url', lakefs_url);
        }
        if (primaryKey) {
            params.append('primary_key', primaryKey);
        }

        const url = `${API_CONFIG.COMPARE_STREAM_ENDPOINT}?${params.toString()}`;
        const eventSource = new EventSource(url);

        if (callbacks.onopen) {
            eventSource.onopen = callbacks.onopen;
        }
        if (callbacks.onmessage) {
            eventSource.onmessage = (event) => {
                try {
                    const parsedData = JSON.parse(event.data);
                    callbacks.onmessage(parsedData);
                } catch (e) {
                    if (callbacks.onerror) {
                        callbacks.onerror(new Error(`Error parsing SSE message data: ${e.message}`));
                    }
                    console.error("Error parsing SSE message data:", event.data, e);
                }
            };
        }
        if (callbacks.onerror) {
            eventSource.onerror = (err) => {
                // EventSource.onerror is called for various reasons, including connection close.
                // Real errors might not have much detail in `err` object itself.
                // The `onclose` callback will be more reliable for actual stream termination.
                callbacks.onerror(err);
                // It's often a good idea to close the EventSource here to prevent auto-reconnection on some errors.
                eventSource.close();
            };
        }
        // Note: EventSource does not have a direct `onclose` like WebSockets.
        // Closure is typically detected by `onerror` or by the server explicitly sending a close event or closing the connection.
        // If an `onclose` callback is provided, it might need to be triggered from `onerror` or after a specific "complete" message.
        if (callbacks.onClose) {
            // A common pattern is to have the server send a special event like { type: "complete" }
            // and then the onmessage handler would call eventSource.close() and callbacks.onClose().
            // Or, if onerror signifies termination, it can call callbacks.onClose().
            // For now, we'll let onerror handle closure indication primarily.
        }

        return eventSource;
    },

    /**
     * Execute NLP query
     * @param {string} repoId - Repository ID
     * @param {string} baseBranch - Base branch name
     * @param {string} query - NLP query
     * @param {string|null} lakefs_url - Optional lakefs URL in format lakefs://repo/branch
     * @param {string|null} compareBranch - Optional compare branch name for diff queries
     * @returns {Promise<Object>} - Query results
     */
    executeNLPQuery: async (repoId, baseBranch, query, lakefs_url = null, compareBranch = null) => {
        try {
            const requestBody = {
                repository: repoId,
                base_branch: baseBranch,
                query: query,
                lakefs_url: lakefs_url,
            };

            if (compareBranch) {
                requestBody.compare_branch = compareBranch;
            }

            const response = await fetch(API_CONFIG.NLP_ENDPOINT, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to execute NLP query');
            }

            return await response.json();
        } catch (error) {
            console.error('Error in executeNLPQuery:', error);
            throw error;
        }
    },
}; 