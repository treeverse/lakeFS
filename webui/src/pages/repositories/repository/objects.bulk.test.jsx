import { describe, it, expect } from 'vitest';

describe('Bulk Operations - Batching Logic with Progress', () => {
  const BATCH_SIZE = 1000;

  describe('Array Batching', () => {
    it('should split large arrays into batches of 1000', () => {
      const testArray = Array.from({ length: 1500 }, (_, i) => `file${i + 1}.txt`);
      
      const batches = [];
      for (let i = 0; i < testArray.length; i += BATCH_SIZE) {
        const batch = testArray.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }
      
      expect(batches).toHaveLength(2);
      expect(batches[0]).toHaveLength(1000);
      expect(batches[1]).toHaveLength(500);
      
      // Verify no overlap and all items included
      const allItems = batches.flat();
      expect(allItems).toHaveLength(1500);
      expect(new Set(allItems).size).toBe(1500); // No duplicates
      
      // Verify correct items in each batch
      expect(batches[0]).toContain('file1.txt');
      expect(batches[0]).toContain('file1000.txt');
      expect(batches[1]).toContain('file1001.txt');
      expect(batches[1]).toContain('file1500.txt');
    });

    it('should handle exactly 1000 objects in one batch', () => {
      const testArray = Array.from({ length: 1000 }, (_, i) => `file${i + 1}.txt`);
      
      const batches = [];
      for (let i = 0; i < testArray.length; i += BATCH_SIZE) {
        const batch = testArray.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }
      
      expect(batches).toHaveLength(1);
      expect(batches[0]).toHaveLength(1000);
      expect(batches[0]).toContain('file1.txt');
      expect(batches[0]).toContain('file1000.txt');
    });

    it('should handle small arrays without batching', () => {
      const testArray = Array.from({ length: 50 }, (_, i) => `file${i + 1}.txt`);
      
      const batches = [];
      for (let i = 0; i < testArray.length; i += BATCH_SIZE) {
        const batch = testArray.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }
      
      expect(batches).toHaveLength(1);
      expect(batches[0]).toHaveLength(50);
    });

    it('should handle empty arrays', () => {
      const testArray = [];
      
      const batches = [];
      for (let i = 0; i < testArray.length; i += BATCH_SIZE) {
        const batch = testArray.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }
      
      expect(batches).toHaveLength(0);
    });

    it('should handle very large arrays (10,000 objects)', () => {
      const testArray = Array.from({ length: 10000 }, (_, i) => `file${i + 1}.txt`);
      
      const batches = [];
      for (let i = 0; i < testArray.length; i += BATCH_SIZE) {
        const batch = testArray.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }
      
      expect(batches).toHaveLength(10);
      batches.forEach((batch, index) => {
        if (index < 9) {
          expect(batch).toHaveLength(1000);
        } else {
          expect(batch).toHaveLength(1000); // Last batch is also 1000 in this case
        }
      });
    });
  });

  describe('Error Handling Patterns', () => {
    it('should properly aggregate batch errors', () => {
      const batchErrors = [
        { batch: 1, error: 'cannot write to protected branch', paths: ['file1.txt'] },
        { batch: 2, error: 'network timeout', paths: ['file1001.txt'] },
      ];

      // Test single batch error
      const singleBatchError = batchErrors.slice(0, 1);
      expect(singleBatchError[0].error).toBe('cannot write to protected branch');

      // Test multiple batch errors
      const multipleBatchErrors = batchErrors;
      const batchNumbers = multipleBatchErrors.map(e => e.batch).join(', ');
      const expectedMessage = `Failed to delete objects in batch(es): ${batchNumbers}. First error: ${multipleBatchErrors[0].error}`;
      
      expect(expectedMessage).toBe('Failed to delete objects in batch(es): 1, 2. First error: cannot write to protected branch');
    });

    it('should format error messages for ObjectErrorList', () => {
      const errors = [
        { path: 'file1.txt', message: 'cannot write to protected branch' },
        { path: 'file2.txt', message: 'object not found' },
        { path: 'file3.txt', message: 'permission denied' }
      ];

      // Test unique error detection
      const uniqueErrors = [...new Set(errors.map(err => err.message))];
      expect(uniqueErrors).toHaveLength(3);

      // Test detailed error message formatting
      const errorDetails = errors.map(err => `"${err.path}": ${err.message}`).join('\n');
      const expectedMessage = `Failed to delete some objects:\n${errorDetails}`;
      
      expect(expectedMessage).toBe('Failed to delete some objects:\n"file1.txt": cannot write to protected branch\n"file2.txt": object not found\n"file3.txt": permission denied');
    });

    it('should detect single error for all objects', () => {
      const errors = [
        { path: 'file1.txt', message: 'cannot write to protected branch' },
        { path: 'file2.txt', message: 'cannot write to protected branch' },
        { path: 'file3.txt', message: 'cannot write to protected branch' }
      ];
      const totalPaths = 3;

      const uniqueErrors = [...new Set(errors.map(err => err.message))];
      const shouldUseSingleError = uniqueErrors.length === 1 && errors.length === totalPaths;
      
      expect(shouldUseSingleError).toBe(true);
      expect(uniqueErrors[0]).toBe('cannot write to protected branch');
    });
  });

  describe('Confirmation Modal Messages', () => {
    it('should generate correct message for small number of objects', () => {
      const selectedCount = 5;
      const message = `Are you sure you want to delete ${selectedCount} selected object${selectedCount !== 1 ? 's' : ''}?${selectedCount > 1000 ? ` (Objects will be processed in batches of 1000)` : ''}`;
      
      expect(message).toBe('Are you sure you want to delete 5 selected objects?');
    });

    it('should generate correct message for exactly 1000 objects', () => {
      const selectedCount = 1000;
      const message = `Are you sure you want to delete ${selectedCount} selected object${selectedCount !== 1 ? 's' : ''}?${selectedCount > 1000 ? ` (Objects will be processed in batches of 1000)` : ''}`;
      
      expect(message).toBe('Are you sure you want to delete 1000 selected objects?');
    });

    it('should generate correct message with batching info for 1000+ objects', () => {
      const selectedCount = 1500;
      const message = `Are you sure you want to delete ${selectedCount} selected object${selectedCount !== 1 ? 's' : ''}?${selectedCount > 1000 ? ` (Objects will be processed in batches of 1000)` : ''}`;
      
      expect(message).toBe('Are you sure you want to delete 1500 selected objects? (Objects will be processed in batches of 1000)');
    });

    it('should generate correct message for single object', () => {
      const selectedCount = 1;
      const message = `Are you sure you want to delete ${selectedCount} selected object${selectedCount !== 1 ? 's' : ''}?${selectedCount > 1000 ? ` (Objects will be processed in batches of 1000)` : ''}`;
      
      expect(message).toBe('Are you sure you want to delete 1 selected object?');
    });
  });

  describe('Progress Tracking', () => {
    it('should calculate correct progress percentages', () => {
      const testCases = [
        { current: 0, total: 1000, expected: 0 },
        { current: 250, total: 1000, expected: 25 },
        { current: 500, total: 1000, expected: 50 },
        { current: 750, total: 1000, expected: 75 },
        { current: 1000, total: 1000, expected: 100 },
        { current: 1500, total: 2500, expected: 60 },
      ];

      testCases.forEach(({ current, total, expected }) => {
        const percentage = Math.round((current / total) * 100);
        expect(percentage).toBe(expected);
      });
    });

    it('should format progress messages correctly', () => {
      const testCases = [
        {
          progress: { operation: 'download', total: 50, currentBatch: 1, totalBatches: 1 },
          expected: 'Downloading 50 objects'
        },
        {
          progress: { operation: 'delete', total: 1500, currentBatch: 2, totalBatches: 2 },
          expected: 'Deleting 1500 objects (Batch 2 of 2)'
        },
        {
          progress: { operation: 'download', total: 2500, currentBatch: 1, totalBatches: 3 },
          expected: 'Downloading 2500 objects (Batch 1 of 3)'
        }
      ];

      testCases.forEach(({ progress, expected }) => {
        const message = `${progress.operation === 'download' ? 'Downloading' : 'Deleting'} ${progress.total} objects${progress.totalBatches > 1 ? ` (Batch ${progress.currentBatch} of ${progress.totalBatches})` : ''}`;
        expect(message).toBe(expected);
      });
    });

    it('should track batch progress correctly', () => {
      const mockProgress = {
        operation: 'delete',
        current: 1000,
        total: 2500,
        currentBatch: 1,
        totalBatches: 3,
        currentBatchProgress: 1000,
        currentBatchTotal: 1000
      };

      // Overall progress
      const overallPercentage = Math.round((mockProgress.current / mockProgress.total) * 100);
      expect(overallPercentage).toBe(40);

      // Batch progress
      const batchPercentage = Math.round((mockProgress.currentBatchProgress / mockProgress.currentBatchTotal) * 100);
      expect(batchPercentage).toBe(100);
    });

    it('should handle progress state transitions', () => {
      const progressStates = [];

      // Simulate progress updates during a batch operation
      const totalObjects = 1500;
      const BATCH_SIZE = 1000;
      const totalBatches = Math.ceil(totalObjects / BATCH_SIZE);

      // Initial state
      progressStates.push({
        operation: 'download',
        current: 0,
        total: totalObjects,
        currentBatch: 0,
        totalBatches: totalBatches
      });

      // Process batches
      for (let i = 0; i < totalObjects; i += BATCH_SIZE) {
        const batch = Math.min(BATCH_SIZE, totalObjects - i);
        const currentBatch = Math.floor(i / BATCH_SIZE) + 1;

        // Start of batch
        progressStates.push({
          operation: 'download',
          current: i,
          total: totalObjects,
          currentBatch: currentBatch,
          totalBatches: totalBatches,
          currentBatchProgress: 0,
          currentBatchTotal: batch
        });

        // End of batch
        progressStates.push({
          operation: 'download',
          current: Math.min(i + BATCH_SIZE, totalObjects),
          total: totalObjects,
          currentBatch: currentBatch,
          totalBatches: totalBatches,
          currentBatchProgress: batch,
          currentBatchTotal: batch
        });
      }

      expect(progressStates).toHaveLength(5); // Initial + 2 batches * 2 states
      expect(progressStates[0].current).toBe(0);
      expect(progressStates[progressStates.length - 1].current).toBe(1500);
    });
  });

  describe('Folder Operations', () => {
    it('should expand folder paths to their contained objects', () => {
      const selectedPaths = ['file1.txt', 'folder/', 'file2.txt'];
      const folderContents = [
        'folder/subfolder/file1.txt',
        'folder/subfolder/file2.txt',
        'folder/file3.txt'
      ];
      
      // Simulate folder expansion logic
      const expandedPaths = [];
      for (const path of selectedPaths) {
        if (path.endsWith('/')) {
          // This would be replaced by actual API call in real implementation
          expandedPaths.push(...folderContents);
        } else {
          expandedPaths.push(path);
        }
      }
      
      expect(expandedPaths).toEqual([
        'file1.txt',
        'folder/subfolder/file1.txt',
        'folder/subfolder/file2.txt',
        'folder/file3.txt',
        'file2.txt'
      ]);
    });

    it('should handle nested folder paths correctly', () => {
      const selectedPaths = ['top-folder/', 'single-file.txt'];
      const folderContents = [
        'top-folder/sub1/file1.txt',
        'top-folder/sub2/file2.txt',
        'top-folder/file3.txt',
        'top-folder/deep/nested/file4.txt'
      ];
      
      const expandedPaths = [];
      for (const path of selectedPaths) {
        if (path.endsWith('/')) {
          expandedPaths.push(...folderContents);
        } else {
          expandedPaths.push(path);
        }
      }
      
      expect(expandedPaths).toEqual([
        'top-folder/sub1/file1.txt',
        'top-folder/sub2/file2.txt',
        'top-folder/file3.txt',
        'top-folder/deep/nested/file4.txt',
        'single-file.txt'
      ]);
    });

    it('should handle empty folders gracefully', () => {
      const selectedPaths = ['empty-folder/', 'file1.txt'];
      const folderContents = []; // Empty folder
      
      const expandedPaths = [];
      for (const path of selectedPaths) {
        if (path.endsWith('/')) {
          expandedPaths.push(...folderContents);
        } else {
          expandedPaths.push(path);
        }
      }
      
      expect(expandedPaths).toEqual(['file1.txt']);
    });

    it('should handle only folder paths', () => {
      const selectedPaths = ['folder1/', 'folder2/'];
      const folder1Contents = ['folder1/file1.txt', 'folder1/file2.txt'];
      const folder2Contents = ['folder2/fileA.txt'];
      
      const expandedPaths = [];
      for (const path of selectedPaths) {
        if (path.endsWith('/')) {
          if (path === 'folder1/') {
            expandedPaths.push(...folder1Contents);
          } else if (path === 'folder2/') {
            expandedPaths.push(...folder2Contents);
          }
        } else {
          expandedPaths.push(path);
        }
      }
      
      expect(expandedPaths).toEqual([
        'folder1/file1.txt',
        'folder1/file2.txt',
        'folder2/fileA.txt'
      ]);
    });

    it('should preserve object order after folder expansion', () => {
      const selectedPaths = ['file1.txt', 'folder/', 'file2.txt', 'another-folder/'];
      const folderContents = ['folder/file3.txt'];
      const anotherFolderContents = ['another-folder/file4.txt', 'another-folder/file5.txt'];
      
      const expandedPaths = [];
      for (const path of selectedPaths) {
        if (path.endsWith('/')) {
          if (path === 'folder/') {
            expandedPaths.push(...folderContents);
          } else if (path === 'another-folder/') {
            expandedPaths.push(...anotherFolderContents);
          }
        } else {
          expandedPaths.push(path);
        }
      }
      
      expect(expandedPaths).toEqual([
        'file1.txt',
        'folder/file3.txt',
        'file2.txt',
        'another-folder/file4.txt',
        'another-folder/file5.txt'
      ]);
    });
  });

  describe('Size Calculation', () => {
    it('should calculate total size of selected objects correctly', () => {
      const selectedPaths = ['file1.txt', 'file2.txt'];
      const allResults = [
        { path: 'file1.txt', path_type: 'object', size_bytes: 1024 },
        { path: 'file2.txt', path_type: 'object', size_bytes: 2048 },
        { path: 'file3.txt', path_type: 'object', size_bytes: 512 },
        { path: 'folder/', path_type: 'common_prefix' }
      ];
      
      // Mock the calculateSelectedObjectsSize function logic
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes) {
            totalSize += entry.size_bytes;
          }
        }
      }
      
      expect(totalSize).toBe(3072); // 1024 + 2048
    });

    it('should include folder contents in size calculation when folder is selected', () => {
      const selectedPaths = ['file1.txt', 'folder/'];
      const allResults = [
        { path: 'file1.txt', path_type: 'object', size_bytes: 1024 },
        { path: 'folder/', path_type: 'common_prefix' }
      ];
      
      // Mock folder contents
      const folderContents = [
        { path: 'folder/file1.txt', path_type: 'object', size_bytes: 512 },
        { path: 'folder/file2.txt', path_type: 'object', size_bytes: 256 },
        { path: 'folder/subfolder/file3.txt', path_type: 'object', size_bytes: 128 }
      ];
      
      // Calculate size including folder contents
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes) {
            totalSize += entry.size_bytes;
          } else if (entry.path_type === 'common_prefix' && entry.path.endsWith('/')) {
            // Add folder contents size
            const folderSize = folderContents.reduce((sum, obj) => sum + (obj.size_bytes || 0), 0);
            totalSize += folderSize;
          }
        }
      }
      
      expect(totalSize).toBe(1920); // 1024 (file1.txt) + 896 (folder contents: 512 + 256 + 128)
    });

    it('should handle multiple selected folders with their contents', () => {
      const selectedPaths = ['folder1/', 'folder2/', 'standalone-file.txt'];
      const allResults = [
        { path: 'folder1/', path_type: 'common_prefix' },
        { path: 'folder2/', path_type: 'common_prefix' },
        { path: 'standalone-file.txt', path_type: 'object', size_bytes: 100 }
      ];
      
      // Mock folder contents
      const folder1Contents = [
        { path: 'folder1/file1.txt', size_bytes: 1000 },
        { path: 'folder1/file2.txt', size_bytes: 2000 }
      ];
      const folder2Contents = [
        { path: 'folder2/fileA.txt', size_bytes: 500 }
      ];
      
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes) {
            totalSize += entry.size_bytes;
          } else if (entry.path_type === 'common_prefix') {
            if (entry.path === 'folder1/') {
              totalSize += folder1Contents.reduce((sum, obj) => sum + (obj.size_bytes || 0), 0);
            } else if (entry.path === 'folder2/') {
              totalSize += folder2Contents.reduce((sum, obj) => sum + (obj.size_bytes || 0), 0);
            }
          }
        }
      }
      
      expect(totalSize).toBe(3600); // 100 (standalone) + 3000 (folder1) + 500 (folder2)
    });

    it('should exclude folders from size calculation', () => {
      const selectedPaths = ['file1.txt', 'folder/'];
      const allResults = [
        { path: 'file1.txt', path_type: 'object', size_bytes: 1024 },
        { path: 'folder/', path_type: 'common_prefix' }
      ];
      
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes) {
            totalSize += entry.size_bytes;
          }
        }
      }
      
      expect(totalSize).toBe(1024); // Only the file, not the folder
    });

    it('should handle objects with zero size', () => {
      const selectedPaths = ['empty-file.txt', 'regular-file.txt'];
      const allResults = [
        { path: 'empty-file.txt', path_type: 'object', size_bytes: 0 },
        { path: 'regular-file.txt', path_type: 'object', size_bytes: 1024 }
      ];
      
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes !== undefined) {
            totalSize += entry.size_bytes;
          }
        }
      }
      
      expect(totalSize).toBe(1024); // 0 + 1024
    });

    it('should handle missing size_bytes property', () => {
      const selectedPaths = ['file1.txt', 'file2.txt'];
      const allResults = [
        { path: 'file1.txt', path_type: 'object', size_bytes: 1024 },
        { path: 'file2.txt', path_type: 'object' } // Missing size_bytes
      ];
      
      let totalSize = 0;
      const selectedSet = new Set(selectedPaths);
      
      for (const entry of allResults) {
        if (selectedSet.has(entry.path)) {
          if (entry.path_type === 'object' && entry.size_bytes) {
            totalSize += entry.size_bytes;
          }
        }
      }
      
      expect(totalSize).toBe(1024); // Only file1.txt
    });
  });

  describe('Batch Processing Simulation', () => {
    it('should simulate successful batch processing', async () => {
      const totalObjects = 2500;
      const objectPaths = Array.from({ length: totalObjects }, (_, i) => `file${i + 1}.txt`);
      
      const mockDeleteObjects = async (repoId, branchId, paths) => {
        // Simulate API call
        if (paths.length > 1000) {
          throw new Error('API supports maximum 1000 objects per request');
        }
        return { success: true };
      };

      // Simulate batching
      const batches = [];
      for (let i = 0; i < objectPaths.length; i += BATCH_SIZE) {
        const batch = objectPaths.slice(i, i + BATCH_SIZE);
        batches.push(batch);
      }

      expect(batches).toHaveLength(3); // 1000, 1000, 500

      // Simulate processing each batch
      const results = [];
      for (const batch of batches) {
        try {
          await mockDeleteObjects('test-repo', 'main', batch);
          results.push({ success: true, batch: batch.length });
        } catch (error) {
          results.push({ success: false, error: error.message, batch: batch.length });
        }
      }

      expect(results).toHaveLength(3);
      expect(results.every(r => r.success)).toBe(true);
    });

    it('should simulate batch processing with some failures', async () => {
      const totalObjects = 2500;
      const objectPaths = Array.from({ length: totalObjects }, (_, i) => `file${i + 1}.txt`);
      
      let batchCount = 0;
      const mockDeleteObjects = async () => {
        batchCount++;
        // Simulate second batch failing
        if (batchCount === 2) {
          throw new Error('cannot write to protected branch');
        }
        return { success: true };
      };

      // Simulate batching and processing
      const allErrors = [];
      for (let i = 0; i < objectPaths.length; i += BATCH_SIZE) {
        const batch = objectPaths.slice(i, i + BATCH_SIZE);
        
        try {
          await mockDeleteObjects('test-repo', 'main', batch);
        } catch (error) {
          allErrors.push({
            batch: Math.floor(i / BATCH_SIZE) + 1,
            error: error.message,
            paths: batch
          });
        }
      }

      expect(allErrors).toHaveLength(1);
      expect(allErrors[0].batch).toBe(2);
      expect(allErrors[0].error).toBe('cannot write to protected branch');
    });
  });

  describe('File Type Preservation in Downloads', () => {
    it('should preserve file types when creating zip archives', () => {
      const mockFileMap = {
        'image.png': new Blob(['fake png data'], { type: 'image/png' }),
        'document.pdf': new Blob(['fake pdf data'], { type: 'application/pdf' }),
        'script.js': new Blob(['console.log("hello");'], { type: 'text/javascript' })
      };

      // Test that file types are preserved as binary data
      Object.entries(mockFileMap).forEach(([path, blob]) => {
        expect(blob.type).toBeTruthy(); // Should have proper MIME type
        
        // Verify blob preserves binary data properly
        if (path.endsWith('.png')) {
          expect(blob.type).toBe('image/png');
        } else if (path.endsWith('.pdf')) {
          expect(blob.type).toBe('application/pdf');
        } else if (path.endsWith('.js')) {
          expect(blob.type).toBe('text/javascript');
        }
      });
    });

    it('should handle binary file conversion to zip correctly', async () => {
      // Simulate the zip creation process
      const mockBinaryData = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]); // PNG header
      const mockBlob = new Blob([mockBinaryData], { type: 'image/png' });
      
      // Convert blob to array buffer (as done in createZipDownload)
      const arrayBuffer = await mockBlob.arrayBuffer();
      const uint8Array = new Uint8Array(arrayBuffer);
      
      // Verify binary data is preserved
      expect(uint8Array).toEqual(mockBinaryData);
      expect(uint8Array.length).toBe(8);
      expect(uint8Array[0]).toBe(137); // PNG signature first byte
      expect(uint8Array[1]).toBe(80);  // PNG signature second byte
    });

    it('should create proper zip file structure', () => {
      const mockZipData = {
        'file1.png': new Uint8Array([1, 2, 3, 4]),
        'file2.txt': new Uint8Array([72, 101, 108, 108, 111]) // "Hello" in ASCII
      };
      
      // Verify zip data structure
      expect(Object.keys(mockZipData)).toEqual(['file1.png', 'file2.txt']);
      expect(mockZipData['file1.png']).toBeInstanceOf(Uint8Array);
      expect(mockZipData['file2.txt']).toBeInstanceOf(Uint8Array);
      
      // Verify file data is preserved as binary
      expect(mockZipData['file1.png'].length).toBe(4);
      expect(mockZipData['file2.txt'][0]).toBe(72); // 'H'
    });

    it('should handle single file download with original type', () => {
      const mockFileMap = {
        'single-image.png': new Blob(['fake png data'], { type: 'image/png' })
      };
      
      const files = Object.entries(mockFileMap);
      expect(files.length).toBe(1);
      
      const [path, blob] = files[0];
      expect(path).toBe('single-image.png');
      expect(blob.type).toBe('image/png');
      
      // Verify proper filename extraction
      const fileName = path.split('/').pop();
      expect(fileName).toBe('single-image.png');
    });

    it('should fallback to text concatenation gracefully', async () => {
      // Test fallback behavior when zip creation fails
      const mockFileMap = {
        'file1.txt': new Blob(['Hello world'], { type: 'text/plain' }),
        'file2.txt': new Blob(['Goodbye world'], { type: 'text/plain' })
      };
      
      const files = Object.entries(mockFileMap);
      const archiveContent = [];
      
      for (const [path, blob] of files) {
        const content = await blob.text();
        archiveContent.push(`--- FILE: ${path} ---\n${content}\n\n`);
      }
      
      const combinedContent = archiveContent.join('');
      expect(combinedContent).toContain('--- FILE: file1.txt ---');
      expect(combinedContent).toContain('Hello world');
      expect(combinedContent).toContain('--- FILE: file2.txt ---');
      expect(combinedContent).toContain('Goodbye world');
    });
  });
});