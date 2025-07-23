import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock fetch globally
window.fetch = vi.fn();

describe('Objects API - deleteObjects with ObjectErrorList', () => {
  let objects;

  beforeEach(async () => {
    fetch.mockClear();
    
    // Import the API module
    const apiModule = await import('./index.js');
    objects = apiModule.objects;
  });

  describe('Successful Deletion', () => {
    it('should handle successful bulk delete with 200 status', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ success: true }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt']);

      expect(fetch).toHaveBeenCalledWith(
        '/api/v1/repositories/test-repo/branches/main/objects/delete',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            paths: ['file1.txt', 'file2.txt']
          }),
        })
      );

      expect(result).toEqual({ success: true });
    });

    it('should handle successful bulk delete with 204 status', async () => {
      const mockResponse = {
        ok: true,
        status: 204,
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['file1.txt']);

      expect(result).toBeNull();
    });

    it('should handle deletion of non-existent objects as success', async () => {
      // In lakeFS, deleting non-existent objects is idempotent and returns success
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ success: true }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['non-existent-file.txt']);

      expect(fetch).toHaveBeenCalledWith(
        '/api/v1/repositories/test-repo/branches/main/objects/delete',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            paths: ['non-existent-file.txt']
          }),
        })
      );

      expect(result).toEqual({ success: true });
    });

    it('should handle mixed deletion of existing and non-existent objects as success', async () => {
      // When some objects exist and some don't, lakeFS still returns success for the overall operation
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ 
          success: true,
          deleted: ['existing-file.txt'] // Only lists actually deleted objects
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['existing-file.txt', 'non-existent-file.txt']);

      expect(result).toEqual({ 
        success: true,
        deleted: ['existing-file.txt']
      });
    });
  });

  describe('ObjectErrorList Handling', () => {
    it('should throw single error when all objects fail with same error', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'file1.txt', message: 'cannot write to protected branch' },
            { path: 'file2.txt', message: 'cannot write to protected branch' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt']))
        .rejects.toThrow('cannot write to protected branch');
    });

    it('should throw detailed error when objects fail with different errors', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'file1.txt', message: 'cannot write to protected branch' },
            { path: 'file2.txt', message: 'object not found' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt']))
        .rejects.toThrow('Failed to delete some objects:\n"file1.txt": cannot write to protected branch\n"file2.txt": object not found');
    });

    it('should throw detailed error when some objects succeed and some fail', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'protected-file.txt', message: 'cannot write to protected branch' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      // Only one object failed out of three attempted
      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt', 'protected-file.txt']))
        .rejects.toThrow('Failed to delete some objects:\n"protected-file.txt": cannot write to protected branch');
    });

    it('should handle empty errors array as success', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: []
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['file1.txt']);

      expect(result).toEqual({ errors: [] });
    });

    it('should handle response without errors property as success', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          success: true,
          deleted: ['file1.txt']
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', ['file1.txt']);

      expect(result).toEqual({ success: true, deleted: ['file1.txt'] });
    });
  });

  describe('HTTP Error Handling', () => {
    it('should throw error for 400 status', async () => {
      const mockResponse = {
        ok: false,
        status: 400,
        headers: {
          get: vi.fn().mockReturnValue('application/json'),
        },
        json: vi.fn().mockResolvedValue({ message: 'Bad request' }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt']))
        .rejects.toThrow('Bad request');
    });

    it('should throw error for 422 status', async () => {
      const mockResponse = {
        ok: false,
        status: 422,
        headers: {
          get: vi.fn().mockReturnValue('application/json'),
        },
        json: vi.fn().mockResolvedValue({ message: 'Validation error' }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt']))
        .rejects.toThrow('Validation error');
    });

    it('should throw error for 500 status', async () => {
      const mockResponse = {
        ok: false,
        status: 500,
        headers: {
          get: vi.fn().mockReturnValue('text/plain'),
        },
        text: vi.fn().mockResolvedValue('Internal server error'),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt']))
        .rejects.toThrow('Internal server error');
    });
  });

  describe('Network Error Handling', () => {
    it('should handle fetch rejection', async () => {
      fetch.mockRejectedValue(new Error('Network error'));

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt']))
        .rejects.toThrow('Network error');
    });

    it('should handle JSON parsing errors', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockRejectedValue(new Error('Invalid JSON')),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt']))
        .rejects.toThrow('Invalid JSON');
    });
  });

  describe('Mixed Error Scenarios', () => {
    it('should handle mixed errors with some objects having different error messages', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'file1.txt', message: 'cannot write to protected branch' },
            { path: 'file2.txt', message: 'object not found' },
            { path: 'file3.txt', message: 'permission denied' },
            { path: 'file4.txt', message: 'cannot write to protected branch' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt']))
        .rejects.toThrow('Failed to delete some objects:\n"file1.txt": cannot write to protected branch\n"file2.txt": object not found\n"file3.txt": permission denied\n"file4.txt": cannot write to protected branch');
    });

    it('should handle large number of mixed errors', async () => {
      const errors = [];
      for (let i = 1; i <= 50; i++) {
        errors.push({
          path: `file${i}.txt`,
          message: i % 3 === 0 ? 'permission denied' : i % 2 === 0 ? 'object not found' : 'cannot write to protected branch'
        });
      }

      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ errors }),
      };
      fetch.mockResolvedValue(mockResponse);

      const paths = Array.from({ length: 50 }, (_, i) => `file${i + 1}.txt`);

      await expect(objects.deleteObjects('test-repo', 'main', paths))
        .rejects.toThrow('Failed to delete some objects:');
    });

    it('should handle errors affecting only some objects in a large batch', async () => {
      // Only 3 objects out of 100 fail
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'protected-file.txt', message: 'cannot write to protected branch' },
            { path: 'missing-file.txt', message: 'object not found' },
            { path: 'readonly-file.txt', message: 'permission denied' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      // Create 100 file paths
      const paths = Array.from({ length: 100 }, (_, i) => `file${i + 1}.txt`);
      paths.push('protected-file.txt', 'missing-file.txt', 'readonly-file.txt');

      await expect(objects.deleteObjects('test-repo', 'main', paths))
        .rejects.toThrow('Failed to delete some objects:\n"protected-file.txt": cannot write to protected branch\n"missing-file.txt": object not found\n"readonly-file.txt": permission denied');
    });

    it('should handle same error message for all objects but with different paths', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'dir1/file.txt', message: 'cannot write to protected branch' },
            { path: 'dir2/file.txt', message: 'cannot write to protected branch' },
            { path: 'dir3/file.txt', message: 'cannot write to protected branch' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['dir1/file.txt', 'dir2/file.txt', 'dir3/file.txt']))
        .rejects.toThrow('cannot write to protected branch');
    });

    it('should handle different error types with similar messages', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'file1.txt', message: 'access denied: insufficient permissions' },
            { path: 'file2.txt', message: 'access denied: user not authorized' },
            { path: 'file3.txt', message: 'access denied: insufficient permissions' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file1.txt', 'file2.txt', 'file3.txt']))
        .rejects.toThrow('Failed to delete some objects:\n"file1.txt": access denied: insufficient permissions\n"file2.txt": access denied: user not authorized\n"file3.txt": access denied: insufficient permissions');
    });
  });

  describe('Batching Scenarios', () => {
    it('should handle API payload for exactly 1000 objects', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ success: true }),
      };
      fetch.mockResolvedValue(mockResponse);

      const paths = Array.from({ length: 1000 }, (_, i) => `file${i + 1}.txt`);
      const result = await objects.deleteObjects('test-repo', 'main', paths);

      expect(fetch).toHaveBeenCalledWith(
        '/api/v1/repositories/test-repo/branches/main/objects/delete',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            paths: paths
          }),
        })
      );

      expect(result).toEqual({ success: true });
    });

    it('should handle ObjectErrorList for large batch with many errors', async () => {
      // Create errors for every 10th object in a 1000-object batch
      const errors = [];
      for (let i = 10; i <= 1000; i += 10) {
        errors.push({
          path: `file${i}.txt`,
          message: 'cannot write to protected branch'
        });
      }

      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ errors }),
      };
      fetch.mockResolvedValue(mockResponse);

      const paths = Array.from({ length: 1000 }, (_, i) => `file${i + 1}.txt`);

      // Since all errors have the same message, should throw single error
      await expect(objects.deleteObjects('test-repo', 'main', paths))
        .rejects.toThrow('cannot write to protected branch');
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty paths array', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ success: true }),
      };
      fetch.mockResolvedValue(mockResponse);

      const result = await objects.deleteObjects('test-repo', 'main', []);

      expect(fetch).toHaveBeenCalledWith(
        '/api/v1/repositories/test-repo/branches/main/objects/delete',
        expect.objectContaining({
          body: JSON.stringify({
            paths: []
          }),
        })
      );

      expect(result).toEqual({ success: true });
    });

    it('should handle special characters in repo and branch names', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({ success: true }),
      };
      fetch.mockResolvedValue(mockResponse);

      await objects.deleteObjects('repo with spaces', 'feature/new-feature', ['file.txt']);

      expect(fetch).toHaveBeenCalledWith(
        '/api/v1/repositories/repo%20with%20spaces/branches/feature%2Fnew-feature/objects/delete',
        expect.objectContaining({
          method: 'POST',
        })
      );
    });

    it('should handle special characters in file paths', async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          errors: [
            { path: 'file with spaces & symbols!.txt', message: 'cannot write to protected branch' }
          ]
        }),
      };
      fetch.mockResolvedValue(mockResponse);

      await expect(objects.deleteObjects('test-repo', 'main', ['file with spaces & symbols!.txt']))
        .rejects.toThrow('cannot write to protected branch');
    });
  });
});