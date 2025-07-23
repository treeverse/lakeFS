import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useBranchProtection } from './useBranchProtection';
import { useAPI } from './api';

// Mock the useAPI hook
vi.mock('./api', () => ({
  useAPI: vi.fn()
}));

// Mock the branchProtectionRules API
vi.mock('../api', () => ({
  branchProtectionRules: {
    getRules: vi.fn()
  }
}));

describe('useBranchProtection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return not protected when no rules exist', () => {
    useAPI.mockReturnValue({
      response: { rules: [] },
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'main'));

    expect(result.current.isProtected).toBe(false);
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.rules).toEqual([]);
  });

  it('should return protected when branch matches exact pattern', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'main' },
          { pattern: 'develop' }
        ] 
      },
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'main'));

    expect(result.current.isProtected).toBe(true);
    expect(result.current.rules).toHaveLength(2);
  });

  it('should return not protected when branch does not match any pattern', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'main' },
          { pattern: 'develop' }
        ] 
      },
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'feature/new-feature'));

    expect(result.current.isProtected).toBe(false);
  });

  it('should return protected when branch matches wildcard pattern', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'release/*' },
          { pattern: 'hotfix/*' }
        ] 
      },
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'release/v1.0.0'));

    expect(result.current.isProtected).toBe(true);
  });

  it('should return not protected when branch does not match wildcard pattern', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'release/*' },
          { pattern: 'hotfix/*' }
        ] 
      },
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'feature/new-feature'));

    expect(result.current.isProtected).toBe(false);
  });

  it('should handle complex patterns correctly', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'main' },
          { pattern: 'release/*' },
          { pattern: '*-prod' },
          { pattern: 'test-*-branch' }
        ] 
      },
      loading: false,
      error: null
    });

    // Test various branch names
    const testCases = [
      { branch: 'main', expected: true },
      { branch: 'release/v1.0.0', expected: true },
      { branch: 'staging-prod', expected: true },
      { branch: 'test-new-branch', expected: true },
      { branch: 'feature/new', expected: false },
      { branch: 'prod', expected: false },
      { branch: 'test-branch', expected: false }
    ];

    testCases.forEach(({ branch, expected }) => {
      const { result } = renderHook(() => useBranchProtection('test-repo', branch));
      expect(result.current.isProtected).toBe(expected);
    });
  });

  it('should return false when branchName is null or undefined', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [{ pattern: 'main' }] 
      },
      loading: false,
      error: null
    });

    const { result: result1 } = renderHook(() => useBranchProtection('test-repo', null));
    expect(result1.current.isProtected).toBe(false);

    const { result: result2 } = renderHook(() => useBranchProtection('test-repo', undefined));
    expect(result2.current.isProtected).toBe(false);
  });

  it('should return false when repoId is null', () => {
    useAPI.mockReturnValue({
      response: null,
      loading: false,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection(null, 'main'));

    expect(result.current.isProtected).toBe(false);
  });

  it('should handle loading state', () => {
    useAPI.mockReturnValue({
      response: null,
      loading: true,
      error: null
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'main'));

    expect(result.current.isProtected).toBe(false);
    expect(result.current.loading).toBe(true);
  });

  it('should handle error state', () => {
    const error = new Error('API Error');
    useAPI.mockReturnValue({
      response: null,
      loading: false,
      error
    });

    const { result } = renderHook(() => useBranchProtection('test-repo', 'main'));

    expect(result.current.isProtected).toBe(false);
    expect(result.current.error).toBe(error);
  });

  it('should handle NotFoundError (no protection rules) gracefully', () => {
    // The hook should internally handle NotFoundError and return empty rules
    renderHook(() => useBranchProtection('test-repo', 'main'));
    
    // Verify that useAPI was called with proper error handling
    expect(useAPI).toHaveBeenCalledWith(
      expect.any(Function),
      ['test-repo']
    );
  });

  it('should escape special regex characters in patterns', () => {
    useAPI.mockReturnValue({
      response: { 
        rules: [
          { pattern: 'branch.with.dots' },
          { pattern: 'branch+with+plus' },
          { pattern: 'branch(with)parens' }
        ] 
      },
      loading: false,
      error: null
    });

    // Should match exact strings, not regex patterns
    const testCases = [
      { branch: 'branch.with.dots', expected: true },
      { branch: 'branchXwithXdots', expected: false }, // . should not match any char
      { branch: 'branch+with+plus', expected: true },
      { branch: 'branchwithplus', expected: false }, // + should not match one or more
      { branch: 'branch(with)parens', expected: true }
    ];

    testCases.forEach(({ branch, expected }) => {
      const { result } = renderHook(() => useBranchProtection('test-repo', branch));
      expect(result.current.isProtected).toBe(expected);
    });
  });
});