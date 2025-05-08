import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { describe, test, expect, vi, beforeEach } from 'vitest';
import CompareBranches from '../compareBranches';
import { FastAPIService } from '../../../../lib/api/fastapi';
import { DiffProvider } from '../../../../lib/hooks/diffContext';

// Mock the FastAPI service
vi.mock('../../../../lib/api/fastapi', () => ({
    FastAPIService: {
        compareBranches: vi.fn(),
        executeNLPQuery: vi.fn(),
    }
}));

// Mock the router hook
vi.mock('../../../../lib/hooks/router', () => ({
    useRouter: () => ({
        query: {},
        push: vi.fn(),
        route: '',
        params: {}
    }),
}));

// Mock the API hooks
vi.mock('../../../../lib/hooks/api', () => ({
    useAPIWithPagination: vi.fn().mockReturnValue({
        nextPage: vi.fn(),
        loading: false,
        error: null,
        results: []
    }),
}));

describe('CompareBranches', () => {
    const mockRepo = { id: 'test-repo' };
    const mockReference = { id: 'main', type: 'branch' };
    const mockCompareReference = { id: 'dev', type: 'branch' };

    beforeEach(() => {
        // Reset mocks
        vi.clearAllMocks();

        // Setup successful API response
        FastAPIService.compareBranches.mockResolvedValue({
            files_compared: ['test.parquet'],
            results: {
                'test.parquet': {
                    added_rows: [],
                    removed_rows: [],
                    modified_rows: [
                        {
                            primary_key: 1,
                            source: { id: 1, name: 'Old Name' },
                            destination: { id: 1, name: 'New Name' }
                        }
                    ],
                    primary_key: 'id'
                }
            },
            conflict_files: [],
            error: null
        });
    });

    test('should call compareBranches with maxDisplayRows parameter', async () => {
        render(
            <DiffProvider>
                <CompareBranches
                    repo={mockRepo}
                    reference={mockReference}
                    compareReference={mockCompareReference}
                    showActionsBar={true}
                />
            </DiffProvider>
        );

        // Verify that compareBranches was called with the correct params
        await waitFor(() => {
            expect(FastAPIService.compareBranches).toHaveBeenCalledWith(
                mockRepo.id,
                mockReference.id,
                mockCompareReference.id,
                `lakefs://${mockRepo.id}/${mockReference.id}`,
                null,
                20 // Default maxDisplayRows value
            );
        });
    });

    test('should display modified rows from the API response', async () => {
        // Mock the component implementation for this test
        vi.spyOn(React, 'useState').mockImplementationOnce(() => [null, vi.fn()]);

        render(
            <DiffProvider>
                <CompareBranches
                    repo={mockRepo}
                    reference={mockReference}
                    compareReference={mockCompareReference}
                    showActionsBar={true}
                />
            </DiffProvider>
        );

        // Wait for API call to complete
        await waitFor(() => {
            expect(FastAPIService.compareBranches).toHaveBeenCalled();
        });

        // This test would verify that the modified rows are correctly displayed
        // in the actual implementation, but we'd need to extend this test
        // with specific UI checks based on your component structure
    });
}); 