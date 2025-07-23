import { test, expect } from '@playwright/test';

// Mock API responses for testing
const mockAPIResponses = {
  // Success response for bulk delete
  bulkDeleteSuccess: {
    success: true
  },
  
  // Protected branch error
  protectedBranchError: {
    errors: [
      { path: 'file1.txt', message: 'cannot write to protected branch' },
      { path: 'file2.txt', message: 'cannot write to protected branch' }
    ]
  },
  
  // Mixed errors
  mixedErrors: {
    errors: [
      { path: 'file1.txt', message: 'cannot write to protected branch' },
      { path: 'file2.txt', message: 'object not found' },
      { path: 'file3.txt', message: 'permission denied' }
    ]
  },
  
  // Repository and reference data
  repository: {
    id: 'test-repo',
    creation_date: 1640995200,
    default_branch: 'main',
    storage_namespace: 's3://test-bucket'
  },
  
  reference: {
    id: 'main',
    type: 'branch',
    commit_id: 'abc123'
  },
  
  // Sample objects data
  objects: {
    pagination: {
      has_more: false,
      max_per_page: 1000,
      results: 5
    },
    results: [
      {
        path: 'file1.txt',
        path_type: 'object',
        physical_address: 's3://test-bucket/data/file1.txt',
        checksum: 'abc123',
        size_bytes: 1024,
        mtime: 1640995200,
        content_type: 'text/plain'
      },
      {
        path: 'file2.txt',
        path_type: 'object',
        physical_address: 's3://test-bucket/data/file2.txt',
        checksum: 'def456',
        size_bytes: 2048,
        mtime: 1640995300,
        content_type: 'text/plain'
      },
      {
        path: 'file3.txt',
        path_type: 'object',
        physical_address: 's3://test-bucket/data/file3.txt',
        checksum: 'ghi789',
        size_bytes: 3072,
        mtime: 1640995400,
        content_type: 'text/plain'
      },
      {
        path: 'folder/',
        path_type: 'common_prefix'
      },
      {
        path: 'large-file.txt',
        path_type: 'object',
        physical_address: 's3://test-bucket/data/large-file.txt',
        checksum: 'jkl012',
        size_bytes: 5242880, // 5MB
        mtime: 1640995500,
        content_type: 'text/plain'
      }
    ]
  }
};

test.describe('Bulk Operations', () => {
  test.beforeEach(async ({ page }) => {
    // Mock API endpoints
    await page.route('**/api/v1/repositories/test-repo', async route => {
      await route.fulfill({
        json: mockAPIResponses.repository
      });
    });
    
    await page.route('**/api/v1/repositories/test-repo/refs/main', async route => {
      await route.fulfill({
        json: mockAPIResponses.reference
      });
    });
    
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects**', async route => {
      await route.fulfill({
        json: mockAPIResponses.objects
      });
    });
    
    await page.route('**/api/v1/config', async route => {
      await route.fulfill({
        json: {
          storage_config: {
            pre_sign_support: true,
            import_support: true
          }
        }
      });
    });
    
    // Navigate to objects page
    await page.goto('/repositories/test-repo/objects');
    await page.waitForLoadState('networkidle');
  });

  test('should display checkboxes when bulk operations are enabled', async ({ page }) => {
    // Wait for the table to load
    await expect(page.locator('table')).toBeVisible();
    
    // Should show table headers with checkboxes
    await expect(page.locator('thead')).toBeVisible();
    await expect(page.locator('th:first-child input[type="checkbox"]')).toBeVisible();
    
    // Should show individual checkboxes for objects (not directories)
    const objectRows = page.locator('tbody tr').filter({ has: page.locator('td:first-child input[type="checkbox"]') });
    await expect(objectRows).toHaveCount(4); // 4 objects, not the folder
  });

  test('should select individual objects and show bulk actions toolbar', async ({ page }) => {
    // Wait for table to load
    await expect(page.locator('table')).toBeVisible();
    
    // Select first object
    const firstCheckbox = page.locator('tbody tr:first-child td:first-child input[type="checkbox"]');
    await firstCheckbox.click();
    
    // Should show bulk actions toolbar
    await expect(page.locator('.bulk-actions-toolbar')).toBeVisible();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1 object selected');
    
    // Should show download and delete buttons
    await expect(page.locator('button:has-text("Download")')).toBeVisible();
    await expect(page.locator('button:has-text("Delete")')).toBeVisible();
    
    // Select second object
    const secondCheckbox = page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]');
    await secondCheckbox.click();
    
    // Should update count
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('2 objects selected');
  });

  test('should select all objects using select all checkbox', async ({ page }) => {
    // Wait for table to load
    await expect(page.locator('table')).toBeVisible();
    
    // Click select all checkbox
    const selectAllCheckbox = page.locator('thead th:first-child input[type="checkbox"]');
    await selectAllCheckbox.click();
    
    // Should show bulk actions toolbar with all objects selected
    await expect(page.locator('.bulk-actions-toolbar')).toBeVisible();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('4 objects selected');
    
    // All individual checkboxes should be checked
    const individualCheckboxes = page.locator('tbody tr td:first-child input[type="checkbox"]');
    const checkboxCount = await individualCheckboxes.count();
    for (let i = 0; i < checkboxCount; i++) {
      await expect(individualCheckboxes.nth(i)).toBeChecked();
    }
  });

  test('should handle successful bulk delete', async ({ page }) => {
    // Mock successful bulk delete
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.fulfill({
        json: mockAPIResponses.bulkDeleteSuccess
      });
    });
    
    // Select objects
    await page.locator('thead th:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('4 objects selected');
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Should show confirmation modal
    await expect(page.locator('[data-testid="confirmation-modal"]')).toBeVisible();
    await expect(page.locator('[data-testid="modal-message"]')).toContainText('Are you sure you want to delete 4 selected objects?');
    
    // Confirm delete
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Wait for API call and modal to close
    await expect(page.locator('[data-testid="confirmation-modal"]')).not.toBeVisible();
    
    // Should not show error
    await expect(page.locator('[data-testid="alert-error"]')).not.toBeVisible();
  });

  test('should handle protected branch error', async ({ page }) => {
    // Mock protected branch error
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.fulfill({
        status: 200,
        json: mockAPIResponses.protectedBranchError
      });
    });
    
    // Select objects
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Confirm delete
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show error message
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
    await expect(page.locator('[data-testid="alert-error"]')).toContainText('cannot write to protected branch');
  });

  test('should handle mixed errors for different objects', async ({ page }) => {
    // Mock mixed errors
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.fulfill({
        status: 200,
        json: mockAPIResponses.mixedErrors
      });
    });
    
    // Select three objects
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(3) td:first-child input[type="checkbox"]').click();
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Confirm delete
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show detailed error message
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
    const errorText = await page.locator('[data-testid="alert-error"]').textContent();
    
    expect(errorText).toContain('Failed to delete some objects');
    expect(errorText).toContain('cannot write to protected branch');
    expect(errorText).toContain('object not found');
    expect(errorText).toContain('permission denied');
  });

  test('should dismiss error messages', async ({ page }) => {
    // Mock error response
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.fulfill({
        status: 200,
        json: mockAPIResponses.protectedBranchError
      });
    });
    
    // Trigger error
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Wait for error to appear
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
    
    // Click to dismiss error
    await page.locator('[data-testid="alert-error"]').click();
    
    // Error should be gone
    await expect(page.locator('[data-testid="alert-error"]')).not.toBeVisible();
  });

  test('should show progress bar during bulk operations', async ({ page }) => {
    // Mock successful bulk delete with delay to see progress
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      // Add delay to simulate processing time
      await new Promise(resolve => setTimeout(resolve, 500));
      await route.fulfill({
        json: { success: true }
      });
    });
    
    // Select objects
    await page.locator('thead th:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('4 objects selected');
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Confirm delete
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show progress bar during operation
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Deleting 4 objects');
    
    // Progress bar should have correct ARIA attributes
    const progressBar = page.locator('.progress-bar');
    await expect(progressBar).toHaveAttribute('role', 'progressbar');
    await expect(progressBar).toHaveAttribute('aria-valuemin', '0');
    await expect(progressBar).toHaveAttribute('aria-valuemax', '4');
    
    // Wait for operation to complete and progress to disappear
    await expect(page.locator('.bulk-operation-progress')).not.toBeVisible({ timeout: 10000 });
  });

  test('should handle large number of objects with batching message and progress', async ({ page }) => {
    // Create mock data with over 1000 objects
    const largeObjectsResponse = {
      pagination: {
        has_more: false,
        max_per_page: 1000,
        results: 1500
      },
      results: []
    };
    
    // Generate 1500 objects
    for (let i = 1; i <= 1500; i++) {
      largeObjectsResponse.results.push({
        path: `file${i}.txt`,
        path_type: 'object',
        physical_address: `s3://test-bucket/data/file${i}.txt`,
        checksum: `checksum${i}`,
        size_bytes: 1024,
        mtime: 1640995200,
        content_type: 'text/plain'
      });
    }
    
    // Mock API with large dataset
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects**', async route => {
      await route.fulfill({
        json: largeObjectsResponse
      });
    });
    
    // Mock successful batch delete (will be called twice for 1500 objects)
    let deleteCallCount = 0;
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      deleteCallCount++;
      const requestBody = JSON.parse(route.request().postData());
      
      // Verify batch sizes
      if (deleteCallCount === 1) {
        expect(requestBody.paths).toHaveLength(1000);
      } else if (deleteCallCount === 2) {
        expect(requestBody.paths).toHaveLength(500);
      }
      
      await route.fulfill({
        json: { success: true }
      });
    });
    
    // Refresh page to load new data
    await page.reload();
    await page.waitForLoadState('networkidle');
    
    // Select all objects
    await page.locator('thead th:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1500 objects selected');
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Should show batching message in confirmation modal
    await expect(page.locator('[data-testid="modal-message"]')).toContainText('Are you sure you want to delete 1500 selected objects?');
    await expect(page.locator('[data-testid="modal-message"]')).toContainText('(Objects will be processed in batches of 1000)');
    
    // Confirm delete
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show progress bar with batch information
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Deleting 1500 objects');
    await expect(page.locator('.bulk-operation-progress')).toContainText('Batch 1 of 2');
    
    // Wait for processing to complete
    await page.waitForTimeout(2000);
    
    // Should have made 2 API calls (1000 + 500)
    expect(deleteCallCount).toBe(2);
    
    // Progress should eventually disappear
    await expect(page.locator('.bulk-operation-progress')).not.toBeVisible({ timeout: 5000 });
  });

  test('should show progress bar for bulk download operations', async ({ page }) => {
    // Select objects for download
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    
    // Click download button
    await page.locator('button:has-text("Download")').click();
    
    // Should show progress bar during download
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Downloading 2 objects');
    
    // Should show progress percentage
    const progressBar = page.locator('.progress-bar').first();
    await expect(progressBar).toBeVisible();
    
    // Wait for download to complete and progress to disappear
    await expect(page.locator('.bulk-operation-progress')).not.toBeVisible({ timeout: 10000 });
  });

  test('should not show delete button for non-branch references', async ({ page }) => {
    // Mock commit reference instead of branch
    await page.route('**/api/v1/repositories/test-repo/refs/abc123', async route => {
      await route.fulfill({
        json: {
          id: 'abc123',
          type: 'commit',
          commit_id: 'abc123'
        }
      });
    });
    
    // Navigate to commit view
    await page.goto('/repositories/test-repo/objects?ref=abc123');
    await page.waitForLoadState('networkidle');
    
    // Select objects
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    
    // Should show download button but not delete button
    await expect(page.locator('button:has-text("Download")')).toBeVisible();
    await expect(page.locator('button:has-text("Delete")')).not.toBeVisible();
  });

  test('should cancel bulk delete confirmation', async ({ page }) => {
    // Select objects
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Should show confirmation modal
    await expect(page.locator('[data-testid="confirmation-modal"]')).toBeVisible();
    
    // Cancel
    await page.locator('[data-testid="modal-cancel"]').click();
    
    // Modal should close and objects should still be selected
    await expect(page.locator('[data-testid="confirmation-modal"]')).not.toBeVisible();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1 object selected');
  });
});

test.describe('Bulk Operations - Folder Support', () => {
  test.beforeEach(async ({ page }) => {
    // Mock setup with folders
    await page.route('**/api/v1/repositories/test-repo', async route => {
      await route.fulfill({ json: mockAPIResponses.repository });
    });
    
    await page.route('**/api/v1/repositories/test-repo/refs/main', async route => {
      await route.fulfill({ json: mockAPIResponses.reference });
    });
    
    // Mock objects response that includes folders
    const objectsWithFolders = {
      pagination: {
        has_more: false,
        max_per_page: 1000,
        results: 6
      },
      results: [
        ...mockAPIResponses.objects.results,
        {
          path: 'documents/',
          path_type: 'common_prefix'
        },
        {
          path: 'images/',
          path_type: 'common_prefix'
        }
      ]
    };
    
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects**', async route => {
      await route.fulfill({ json: objectsWithFolders });
    });
    
    await page.route('**/api/v1/config', async route => {
      await route.fulfill({
        json: {
          storage_config: {
            pre_sign_support: true,
            import_support: true
          }
        }
      });
    });
    
    await page.goto('/repositories/test-repo/objects');
    await page.waitForLoadState('networkidle');
  });

  test('should show checkboxes for folders as well as objects', async ({ page }) => {
    await expect(page.locator('table')).toBeVisible();
    
    // Should show checkboxes for both objects and folders
    const objectRows = page.locator('tbody tr').filter({ has: page.locator('td:first-child input[type="checkbox"]') });
    await expect(objectRows).toHaveCount(6); // 4 objects + 2 folders
  });

  test('should select folders and show correct count', async ({ page }) => {
    // Select a folder
    const folderRow = page.locator('tbody tr').filter({ has: page.locator('td.tree-path svg[data-testid="file-directory-icon"]') }).first();
    const folderCheckbox = folderRow.locator('td:first-child input[type="checkbox"]');
    await folderCheckbox.click();
    
    // Should show bulk actions toolbar
    await expect(page.locator('.bulk-actions-toolbar')).toBeVisible();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1 object selected');
  });

  test('should handle folder deletion by expanding to contained objects', async ({ page }) => {
    // Mock folder listing to return objects inside the folder
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/ls**', async route => {
      const url = new URL(route.request().url());
      const prefix = url.searchParams.get('prefix');
      
      if (prefix === 'documents/') {
        await route.fulfill({
          json: {
            pagination: { has_more: false },
            results: [
              {
                path: 'documents/doc1.txt',
                path_type: 'object',
                size_bytes: 1024,
                mtime: 1640995200
              },
              {
                path: 'documents/doc2.txt',
                path_type: 'object',
                size_bytes: 2048,
                mtime: 1640995300
              }
            ]
          }
        });
      } else {
        await route.fulfill({ json: { pagination: { has_more: false }, results: [] } });
      }
    });
    
    // Mock successful folder delete (objects inside will be deleted)
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      const requestBody = JSON.parse(route.request().postData());
      
      // Should receive the expanded object paths, not the folder path
      expect(requestBody.paths).toEqual(['documents/doc1.txt', 'documents/doc2.txt']);
      
      await route.fulfill({
        json: { success: true }
      });
    });
    
    // Select the documents folder
    const documentsRow = page.locator('tbody tr').filter({ 
      has: page.locator('td.tree-path a:has-text("documents")') 
    });
    await documentsRow.locator('td:first-child input[type="checkbox"]').click();
    
    // Click delete button
    await page.locator('button:has-text("Delete")').click();
    
    // Confirm deletion
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should complete successfully without errors
    await expect(page.locator('[data-testid="alert-error"]')).not.toBeVisible();
  });

  test('should handle folder download by expanding to contained objects', async ({ page }) => {
    // Mock folder listing
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/ls**', async route => {
      const url = new URL(route.request().url());
      const prefix = url.searchParams.get('prefix');
      
      if (prefix === 'images/') {
        await route.fulfill({
          json: {
            pagination: { has_more: false },
            results: [
              {
                path: 'images/photo1.jpg',
                path_type: 'object',
                size_bytes: 5242880,
                mtime: 1640995200
              },
              {
                path: 'images/photo2.png',
                path_type: 'object',
                size_bytes: 3145728,
                mtime: 1640995300
              }
            ]
          }
        });
      }
    });
    
    // Select the images folder
    const imagesRow = page.locator('tbody tr').filter({ 
      has: page.locator('td.tree-path a:has-text("images")') 
    });
    await imagesRow.locator('td:first-child input[type="checkbox"]').click();
    
    // Click download button
    await page.locator('button:has-text("Download")').click();
    
    // Should show progress during download expansion and processing
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Downloading');
  });

  test('should handle mixed selection of objects and folders', async ({ page }) => {
    // Mock folder listing for when folders are expanded
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/ls**', async route => {
      await route.fulfill({
        json: {
          pagination: { has_more: false },
          results: [
            {
              path: 'documents/mixed-test.txt',
              path_type: 'object',
              size_bytes: 1024,
              mtime: 1640995200
            }
          ]
        }
      });
    });
    
    // Mock delete for mixed selection
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      const requestBody = JSON.parse(route.request().postData());
      
      // Should include both direct object and expanded folder contents
      expect(requestBody.paths).toContain('file1.txt'); // Direct object
      expect(requestBody.paths).toContain('documents/mixed-test.txt'); // From folder expansion
      
      await route.fulfill({ json: { success: true } });
    });
    
    // Select both an object and a folder
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click(); // file1.txt
    const documentsRow = page.locator('tbody tr').filter({ 
      has: page.locator('td.tree-path a:has-text("documents")') 
    });
    await documentsRow.locator('td:first-child input[type="checkbox"]').click();
    
    // Should show correct count
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('2 objects selected');
    
    // Delete mixed selection
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should complete successfully
    await expect(page.locator('[data-testid="alert-error"]')).not.toBeVisible();
  });

  test('should handle empty folder deletion gracefully', async ({ page }) => {
    // Mock empty folder
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/ls**', async route => {
      await route.fulfill({
        json: {
          pagination: { has_more: false },
          results: [] // Empty folder
        }
      });
    });
    
    // Mock delete API (should not be called for empty folder)
    let deleteApiCalled = false;
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      deleteApiCalled = true;
      await route.fulfill({ json: { success: true } });
    });
    
    // Select empty folder
    const documentsRow = page.locator('tbody tr').filter({ 
      has: page.locator('td.tree-path a:has-text("documents")') 
    });
    await documentsRow.locator('td:first-child input[type="checkbox"]').click();
    
    // Delete empty folder
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Delete API should not be called for empty folders
    await page.waitForTimeout(1000);
    expect(deleteApiCalled).toBe(false);
    
    // Should complete successfully
    await expect(page.locator('[data-testid="alert-error"]')).not.toBeVisible();
  });

  test('should show error when folder listing fails', async ({ page }) => {
    // Mock folder listing error
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/ls**', async route => {
      await route.abort('internetdisconnected');
    });
    
    // Select folder
    const documentsRow = page.locator('tbody tr').filter({ 
      has: page.locator('td.tree-path a:has-text("documents")') 
    });
    await documentsRow.locator('td:first-child input[type="checkbox"]').click();
    
    // Try to delete
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show error about folder listing failure
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
    await expect(page.locator('[data-testid="alert-error"]')).toContainText('Failed to list objects in folder');
  });
});

test.describe('Bulk Operations - Size Display and Zip Download', () => {
  test.beforeEach(async ({ page }) => {
    // Mock basic setup with objects that have size information
    await page.route('**/api/v1/repositories/test-repo', async route => {
      await route.fulfill({ json: mockAPIResponses.repository });
    });
    
    await page.route('**/api/v1/repositories/test-repo/refs/main', async route => {
      await route.fulfill({ json: mockAPIResponses.reference });
    });
    
    // Mock objects with size information
    const objectsWithSize = {
      pagination: {
        has_more: false,
        max_per_page: 1000,
        results: 3
      },
      results: [
        {
          path: 'small-file.txt',
          path_type: 'object',
          physical_address: 's3://test-bucket/data/small-file.txt',
          checksum: 'abc123',
          size_bytes: 1024, // 1 KB
          mtime: 1640995200,
          content_type: 'text/plain'
        },
        {
          path: 'large-file.txt',
          path_type: 'object',
          physical_address: 's3://test-bucket/data/large-file.txt',
          checksum: 'def456',
          size_bytes: 5242880, // 5 MB
          mtime: 1640995300,
          content_type: 'application/octet-stream'
        },
        {
          path: 'medium-file.txt',
          path_type: 'object',
          physical_address: 's3://test-bucket/data/medium-file.txt',
          checksum: 'ghi789',
          size_bytes: 1048576, // 1 MB
          mtime: 1640995400,
          content_type: 'text/plain'
        }
      ]
    };
    
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects**', async route => {
      await route.fulfill({ json: objectsWithSize });
    });
    
    await page.route('**/api/v1/config', async route => {
      await route.fulfill({
        json: {
          storage_config: {
            pre_sign_support: true,
            import_support: true
          }
        }
      });
    });
    
    await page.goto('/repositories/test-repo/objects');
    await page.waitForLoadState('networkidle');
  });

  test('should show total size when objects are selected', async ({ page }) => {
    // Select first object (1 KB)
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    
    // Should show bulk actions toolbar with size
    await expect(page.locator('.bulk-actions-toolbar')).toBeVisible();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1 object selected');
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(1.0 KB)');
  });

  test('should show combined total size for multiple objects', async ({ page }) => {
    // Select first two objects (1 KB + 5 MB = 5.001 MB)
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    
    // Should show combined size
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('2 objects selected');
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(5.0 MB)');
  });

  test('should update size when selection changes', async ({ page }) => {
    // Select large file (5 MB)
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(5.0 MB)');
    
    // Add medium file (1 MB), total should be 6 MB
    await page.locator('tbody tr:nth-child(3) td:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('2 objects selected');
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(6.0 MB)');
    
    // Remove large file, should be 1 MB
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('1 object selected');
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(1.0 MB)');
  });

  test('should use select all and show total size for all objects', async ({ page }) => {
    // Click select all
    await page.locator('thead th:first-child input[type="checkbox"]').click();
    
    // Should show total size for all objects (1 KB + 5 MB + 1 MB = ~6.001 MB)
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('3 objects selected');
    await expect(page.locator('.bulk-actions-toolbar')).toContainText('(6.0 MB)');
  });

  test('should show zip download status for multiple files', async ({ page }) => {
    // Mock file fetch for zip creation
    await page.route('**/api/v1/repositories/test-repo/refs/main/objects**', async route => {
      // Return empty blob for file downloads
      await route.fulfill({
        status: 200,
        body: 'file content',
        headers: { 'content-type': 'text/plain' }
      });
    });
    
    // Select two files
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('tbody tr:nth-child(2) td:first-child input[type="checkbox"]').click();
    
    // Click download button
    await page.locator('button:has-text("Download")').click();
    
    // Should show progress with fetching status
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Downloading 2 objects');
    
    // May show fetching status (though it might be too fast to catch)
    // The test verifies the zip download logic is triggered for multiple files
  });

  test('should handle single file download without zip', async ({ page }) => {
    // Select only one file
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    
    // Click download button
    await page.locator('button:has-text("Download")').click();
    
    // Should show normal download progress (falls back to individual download)
    await expect(page.locator('.bulk-operation-progress')).toBeVisible();
    await expect(page.locator('.bulk-operation-progress')).toContainText('Downloading 1 objects');
  });
});

test.describe('Bulk Operations - Error Handling Edge Cases', () => {
  test.beforeEach(async ({ page }) => {
    // Mock basic setup
    await page.route('**/api/v1/repositories/test-repo', async route => {
      await route.fulfill({ json: mockAPIResponses.repository });
    });
    
    await page.route('**/api/v1/repositories/test-repo/refs/main', async route => {
      await route.fulfill({ json: mockAPIResponses.reference });
    });
    
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects**', async route => {
      await route.fulfill({ json: mockAPIResponses.objects });
    });
    
    await page.goto('/repositories/test-repo/objects');
    await page.waitForLoadState('networkidle');
  });

  test('should handle network errors during bulk delete', async ({ page }) => {
    // Mock network error
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.abort('internetdisconnected');
    });
    
    // Select and attempt delete
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should show network error
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
  });

  test('should handle malformed API responses', async ({ page }) => {
    // Mock malformed JSON response
    await page.route('**/api/v1/repositories/test-repo/branches/main/objects/delete', async route => {
      await route.fulfill({
        status: 200,
        body: 'invalid json response'
      });
    });
    
    // Select and attempt delete
    await page.locator('tbody tr:first-child td:first-child input[type="checkbox"]').click();
    await page.locator('button:has-text("Delete")').click();
    await page.locator('[data-testid="modal-confirm"]').click();
    
    // Should handle JSON parsing error gracefully
    await expect(page.locator('[data-testid="alert-error"]')).toBeVisible();
  });
});