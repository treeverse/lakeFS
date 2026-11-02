import React from 'react';
import { describe, expect, it } from 'vitest';
import { render, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router-dom';
import { PageTitle, RepositoryPageTitle } from './pageTitle';

const Navigation = () => {
    const navigate = useNavigate();
    return (
        <>
            <button onClick={() => navigate('/auth/users')}>Administration</button>
            <button onClick={() => navigate('/datasets')}>Datasets</button>
            <button onClick={() => navigate('/auth/login')}>Login</button>
            <button onClick={() => navigate('/setup')}>Setup</button>
        </>
    );
};

describe('page titles', () => {
    it.each([
        ['/repositories/my-repo/objects', { id: 'main', type: 'branch' }, 'my-repo / main | lakeFS'],
        ['/repositories/my-repo/object', { id: 'feature', type: 'branch' }, 'my-repo / feature | lakeFS'],
        ['/repositories/my-repo/objects', { id: 'v1', type: 'tag' }, 'my-repo | lakeFS'],
        ['/repositories/my-repo/branches', { id: 'main', type: 'branch' }, 'my-repo | lakeFS'],
        ['/repositories/my-repo/pulls', { id: 'main', type: 'branch' }, 'Pull Requests - my-repo | lakeFS'],
        ['/repositories/my-repo/pulls/create', null, 'Create Pull Request - my-repo | lakeFS'],
        ['/repositories/my-repo/pulls/42', null, 'Pull Request - my-repo | lakeFS'],
    ])('sets title for %s', (path, reference, expected) => {
        render(
            <MemoryRouter initialEntries={[path]}>
                <RepositoryPageTitle repoId="my-repo" repo={{ id: 'my-repo' }} reference={reference} />
            </MemoryRouter>,
        );
        expect(document.title).toBe(expected);
    });

    it('updates the title when navigating between sections', async () => {
        const { getByRole } = render(
            <MemoryRouter initialEntries={['/repositories']}>
                <Navigation />
                <Routes>
                    <Route path="/repositories" element={<PageTitle title="Repositories" />} />
                    <Route path="/auth/users" element={<PageTitle title="Administration" />} />
                    <Route path="/datasets" element={<PageTitle title="Datasets" />} />
                    <Route path="/auth/login" element={<PageTitle title="Login" />} />
                    <Route path="/setup" element={<PageTitle title="Setup" />} />
                </Routes>
            </MemoryRouter>,
        );
        expect(document.title).toBe('Repositories | lakeFS');
        fireEvent.click(getByRole('button', { name: 'Administration' }));
        await waitFor(() => expect(document.title).toBe('Administration | lakeFS'));
        fireEvent.click(getByRole('button', { name: 'Datasets' }));
        await waitFor(() => expect(document.title).toBe('Datasets | lakeFS'));
        fireEvent.click(getByRole('button', { name: 'Login' }));
        await waitFor(() => expect(document.title).toBe('Login | lakeFS'));
        fireEvent.click(getByRole('button', { name: 'Setup' }));
        await waitFor(() => expect(document.title).toBe('Setup | lakeFS'));
    });
});
