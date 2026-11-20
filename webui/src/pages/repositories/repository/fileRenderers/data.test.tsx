import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

const mocks = vi.hoisted(() => ({ runDuckDBQuery: vi.fn() }));

vi.mock('./duckdb', () => ({ runDuckDBQuery: mocks.runDuckDBQuery }));

import { DuckDBRenderer } from './data';

const INITIAL_QUERY = "SELECT * FROM READ_PARQUET('lakefs://repo/main/data.parquet', hive_partitioning=false) LIMIT 20";
const EDITED_QUERY = 'SELECT 42';

const renderRenderer = () =>
    render(<DuckDBRenderer repoId="repo" refId="main" path="data.parquet" fileExtension="parquet" sizeBytes={1024} />);

const getEditor = () => screen.getByRole('textbox') as HTMLTextAreaElement;

/** Replace the editor contents, the way a user editing the query would. */
const replaceQuery = async (user: ReturnType<typeof userEvent.setup>, sql: string) => {
    const editor = getEditor();
    await user.clear(editor);
    await user.click(editor);
    await user.type(editor, sql);
    expect(getEditor().value).toBe(sql);
};

describe('DuckDBRenderer query execution', () => {
    beforeEach(() => {
        mocks.runDuckDBQuery.mockReset();
        mocks.runDuckDBQuery.mockResolvedValue(null);
    });

    it('runs the initial query on mount', async () => {
        renderRenderer();

        await waitFor(() => expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(1));
        expect(mocks.runDuckDBQuery).toHaveBeenCalledWith(INITIAL_QUERY);
    });

    it('runs the edited query, not a stale one, when Execute is pressed', async () => {
        const user = userEvent.setup();
        renderRenderer();
        await waitFor(() => expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(1));

        await replaceQuery(user, EDITED_QUERY);
        // Editing alone must not re-run the query, otherwise every keystroke would hit DuckDB.
        expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(1);

        await user.click(screen.getByRole('button', { name: /execute/i }));

        await waitFor(() => expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(2));
        expect(mocks.runDuckDBQuery).toHaveBeenLastCalledWith(EDITED_QUERY);
    });

    it('runs the edited query, not a stale one, when the editor fires Run with Ctrl+Enter', async () => {
        const user = userEvent.setup();
        renderRenderer();
        await waitFor(() => expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(1));

        await replaceQuery(user, EDITED_QUERY);
        expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(1);

        fireEvent.keyDown(getEditor(), { key: 'Enter', ctrlKey: true });

        await waitFor(() => expect(mocks.runDuckDBQuery).toHaveBeenCalledTimes(2));
        expect(mocks.runDuckDBQuery).toHaveBeenLastCalledWith(EDITED_QUERY);
    });
});
