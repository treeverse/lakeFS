import React from 'react';
import { useAPI } from './api';
import { branchProtectionRules } from '../api';

/**
 * Hook to check if a branch is protected by branch protection rules
 * @param {string} repoId - Repository ID
 * @param {string} branchName - Branch name to check
 * @returns {object} Object containing isProtected boolean and loading state
 */
export function useBranchProtection(repoId, branchName) {
  const { response: rulesResponse, loading, error } = useAPI(async () => {
    if (!repoId) return null;
    try {
      return await branchProtectionRules.getRules(repoId);
    } catch (err) {
      // If no protection rules exist, treat as not protected
      if (err.name === 'NotFoundError') {
        return { rules: [] };
      }
      throw err;
    }
  }, [repoId]);

  const isProtected = React.useMemo(() => {
    if (!rulesResponse || !branchName) return false;
    
    const rules = rulesResponse.rules || [];
    return rules.some(rule => {
      const pattern = rule.pattern;
      
      // Convert glob pattern to regex
      // Simple implementation that supports * wildcards
      const regexPattern = pattern
        .replace(/[.+^${}()|[\]\\]/g, '\\$&') // Escape special regex chars
        .replace(/\*/g, '.*'); // Convert * to .*
      
      const regex = new RegExp(`^${regexPattern}$`);
      return regex.test(branchName);
    });
  }, [rulesResponse, branchName]);

  return {
    isProtected,
    loading,
    error,
    rules: rulesResponse?.rules || []
  };
}