import { renderHook, act } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { useExpandCollapseDirs } from './useExpandCollapseDirs';

describe('useExpandCollapseDirs', () => {
    it('returns null initially for isAllExpanded', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());
        expect(result.current.isAllExpanded).toBe(null);
    });

    it('sets isAllExpanded to true when expandAll is called', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.expandAll();
        });

        expect(result.current.isAllExpanded).toBe(true);
    });

    it('sets isAllExpanded to false when collapseAll is called', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.collapseAll();
        });

        expect(result.current.isAllExpanded).toBe(false);
    });

    it('tracks manually toggled paths', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.markDirAsManuallyToggled('folder/data');
        });

        expect(result.current.wasDirManuallyToggled('folder/data')).toBe(true);
        expect(result.current.wasDirManuallyToggled('folder/other')).toBe(false);
    });

    it('resets manually toggled dirs when expandAll is called', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.markDirAsManuallyToggled('folder/data');
            result.current.expandAll();
        });

        expect(result.current.wasDirManuallyToggled('folder/data')).toBe(false);
    });

    it('does not duplicate paths in manuallyToggledDirs when marked twice', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.markDirAsManuallyToggled('folder/data');
            result.current.markDirAsManuallyToggled('folder/data');
        });

        expect(result.current.wasDirManuallyToggled('folder/data')).toBe(true);
    });

    it('resets both expand state and manual toggles when collapseAll is called after expandAll', () => {
        const { result } = renderHook(() => useExpandCollapseDirs());

        act(() => {
            result.current.expandAll();
            result.current.markDirAsManuallyToggled('folder/data');
            result.current.collapseAll();
        });

        expect(result.current.isAllExpanded).toBe(false);
        expect(result.current.wasDirManuallyToggled('folder/data')).toBe(false);
    });
});
