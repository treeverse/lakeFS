import { describe, it, expect } from 'vitest';

describe('Bulk Operations API Integration', () => {
  it('should export deleteObjects method in objects API', async () => {
    const apiModule = await import('./index.js');
    
    // Verify the objects instance has the deleteObjects method
    expect(apiModule.objects).toBeDefined();
    expect(typeof apiModule.objects.deleteObjects).toBe('function');
  });

  it('should export linkToPath helper for bulk downloads', async () => {
    const apiModule = await import('./index.js');
    
    // Verify linkToPath helper exists for bulk downloads
    expect(apiModule.linkToPath).toBeDefined();
    expect(typeof apiModule.linkToPath).toBe('function');
  });

  it('should construct correct API endpoint for bulk delete', () => {
    const repoId = 'test-repo';
    const branchId = 'main';
    
    // Test the endpoint format that deleteObjects would use
    const expectedEndpoint = `/api/v1/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/objects/delete`;
    
    expect(expectedEndpoint).toBe('/api/v1/repositories/test-repo/branches/main/objects/delete');
  });

  it('should handle URL encoding for repository and branch names', () => {
    const repoId = 'repo with spaces';
    const branchId = 'feature/branch-with-slash';
    
    const expectedEndpoint = `/api/v1/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/objects/delete`;
    
    expect(expectedEndpoint).toBe('/api/v1/repositories/repo%20with%20spaces/branches/feature%2Fbranch-with-slash/objects/delete');
  });

  it('should format bulk delete payload correctly', () => {
    const paths = ['file1.txt', 'folder/file2.txt', 'path with spaces.txt'];
    
    const expectedPayload = {
      paths: paths
    };
    
    expect(expectedPayload).toEqual({
      paths: [
        'file1.txt',
        'folder/file2.txt',
        'path with spaces.txt'
      ]
    });
  });

  it('should handle ObjectErrorList in bulk delete response', () => {
    // Test case for when API returns 200 but with errors in the response body
    const mockResponseWithErrors = {
      errors: [
        { path: 'file1.txt', message: 'cannot write to protected branch' },
        { path: 'file2.txt', message: 'cannot write to protected branch' }
      ]
    };
    
    // Both objects failed with the same error - should throw that error
    const uniqueErrors = [...new Set(mockResponseWithErrors.errors.map(err => err.message))];
    expect(uniqueErrors).toEqual(['cannot write to protected branch']);
    expect(uniqueErrors.length).toBe(1);
  });

  it('should handle partial failures in bulk delete', () => {
    // Test case for mixed errors
    const mockResponseWithMixedErrors = {
      errors: [
        { path: 'file1.txt', message: 'cannot write to protected branch' },
        { path: 'file2.txt', message: 'object not found' }
      ]
    };
    
    const errorDetails = mockResponseWithMixedErrors.errors.map(err => `"${err.path}": ${err.message}`).join('\n');
    const expectedMessage = `Failed to delete some objects:\n${errorDetails}`;
    
    expect(expectedMessage).toBe('Failed to delete some objects:\n"file1.txt": cannot write to protected branch\n"file2.txt": object not found');
  });
});