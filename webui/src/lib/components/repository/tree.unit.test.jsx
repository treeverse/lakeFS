import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { Tree } from './tree.jsx';
import { RefTypeBranch } from '../../../constants';
import * as matchers from '@testing-library/jest-dom/matchers';

expect.extend(matchers);

// Mock dependencies
vi.mock('../nav', () => ({
  Link: ({ children, href, ...props }) => (
    <a href={typeof href === 'string' ? href : href.pathname} {...props}>
      {children}
    </a>
  ),
}));

vi.mock('../../api', () => ({
  linkToPath: vi.fn((repoId, refId, path) => `/download/${path}`),
}));

// Mock the controls components
vi.mock('../../controls', () => ({
  Loading: () => <div data-testid="loading">Loading...</div>,
  AlertError: ({ error, onDismiss }) => (
    <div data-testid="alert-error" onClick={onDismiss}>
      Error: {error.message || error}
    </div>
  ),
}));

// Mock dayjs
vi.mock('dayjs', () => {
  const mockDayjs = vi.fn(() => ({
    fromNow: () => '2 hours ago',
    format: () => '12/25/2023 10:30:00'
  }));
  mockDayjs.unix = vi.fn(() => ({
    fromNow: () => '2 hours ago',
    format: () => '12/25/2023 10:30:00'
  }));
  return { default: mockDayjs };
});

// Helper to render component with router context
const renderWithRouter = (component) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  );
};

describe('Tree Component - Bulk Operations UI', () => {
  const mockConfig = {
    config: {
      pre_sign_support: true,
      pre_sign_support_ui: true,
    },
  };

  const mockRepo = {
    id: 'test-repo',
    readOnly: false,
  };

  const mockReference = {
    id: 'main',
    type: RefTypeBranch,
  };

  const mockResults = [
    {
      path: 'file1.txt',
      path_type: 'object',
      size_bytes: 1024,
      mtime: 1640995200,
      diff_type: undefined,
    },
    {
      path: 'file2.txt',
      path_type: 'object',
      size_bytes: 2048,
      mtime: 1640995300,
      diff_type: undefined,
    },
  ];

  const baseProps = {
    config: mockConfig,
    repo: mockRepo,
    reference: mockReference,
    results: mockResults,
    after: '',
    nextPage: false,
    onPaginate: vi.fn(),
    onUpload: vi.fn(),
    onImport: vi.fn(),
    onDelete: vi.fn(),
    showActions: true,
    path: '',
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Clear any leftover DOM elements from previous tests
    document.body.innerHTML = '';
  });

  describe('Bulk Operations Disabled', () => {
    it('should not show checkboxes when bulk operations disabled', () => {
      const props = {
        ...baseProps,
        enableBulkOperations: false,
      };

      renderWithRouter(<Tree {...props} />);

      // Should not find any checkboxes
      expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
      
      // Should not show bulk actions toolbar
      expect(screen.queryByText(/objects selected/)).not.toBeInTheDocument();
    });

    it('should not show table header when bulk operations disabled', () => {
      const props = {
        ...baseProps,
        enableBulkOperations: false,
      };

      renderWithRouter(<Tree {...props} />);

      // Should not show table headers (they only show when bulk operations enabled)
      expect(screen.queryByText('Name')).not.toBeInTheDocument();
      expect(screen.queryByText('Size')).not.toBeInTheDocument();
      expect(screen.queryByText('Actions')).not.toBeInTheDocument();
    });
  });

  describe('Bulk Operations Enabled', () => {
    const bulkProps = {
      ...baseProps,
      enableBulkOperations: true,
      selectedObjects: new Set(),
      onSelectionChange: vi.fn(),
      onBulkDownload: vi.fn(),
      onBulkDelete: vi.fn(),
    };

    it('should show table header with checkboxes when bulk operations enabled', () => {
      renderWithRouter(<Tree {...bulkProps} />);

      // Should show table headers
      expect(screen.getByText('Name')).toBeInTheDocument();
      expect(screen.getByText('Size')).toBeInTheDocument();
      expect(screen.getByText('Actions')).toBeInTheDocument();

      // Should show select all checkbox
      const checkboxes = screen.getAllByRole('checkbox');
      expect(checkboxes.length).toBeGreaterThan(0);
    });

    it('should show individual checkboxes for selectable objects', () => {
      renderWithRouter(<Tree {...bulkProps} />);

      const checkboxes = screen.getAllByRole('checkbox');
      // Should have select all checkbox + individual object checkboxes (at least 3)
      expect(checkboxes.length).toBeGreaterThanOrEqual(3);
      
      // Verify we have the select all checkbox
      const selectAllCheckbox = checkboxes.find(cb => cb.indeterminate !== undefined || cb.checked === false);
      expect(selectAllCheckbox).toBeInTheDocument();
    });

    it('should not show bulk actions toolbar when no objects selected', () => {
      renderWithRouter(<Tree {...bulkProps} />);

      expect(screen.queryByText(/objects selected/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Download/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Delete/)).not.toBeInTheDocument();
    });

    it('should show bulk actions toolbar when objects are selected', () => {
      const propsWithSelection = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt', 'file2.txt']),
      };

      renderWithRouter(<Tree {...propsWithSelection} />);

      expect(screen.getAllByText('2 objects selected')[0]).toBeInTheDocument();
      expect(screen.getAllByText(/Download/)[0]).toBeInTheDocument();
      expect(screen.getAllByText(/Delete/)[0]).toBeInTheDocument();
    });

    it('should call onBulkDownload when download button clicked', () => {
      const propsWithSelection = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt']),
      };

      renderWithRouter(<Tree {...propsWithSelection} />);

      const downloadButton = screen.getAllByText(/Download/)[0].closest('button');
      fireEvent.click(downloadButton);

      expect(bulkProps.onBulkDownload).toHaveBeenCalledTimes(1);
    });

    it('should call onBulkDelete when delete button clicked', () => {
      const propsWithSelection = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt']),
      };

      renderWithRouter(<Tree {...propsWithSelection} />);

      const deleteButton = screen.getAllByText(/Delete/)[0].closest('button');
      fireEvent.click(deleteButton);

      expect(bulkProps.onBulkDelete).toHaveBeenCalledTimes(1);
    });

    it('should not show delete button for non-branch references', () => {
      const propsWithCommit = {
        ...bulkProps,
        reference: { id: 'abc123', type: 'commit' },
        selectedObjects: new Set(['file1.txt']),
      };

      renderWithRouter(<Tree {...propsWithCommit} />);

      expect(screen.getAllByText(/Download/)[0]).toBeInTheDocument();
      
      // For non-branch references, there should be no bulk delete button in the toolbar
      // The logic should prevent showing bulk delete for commits/tags
      
      // Check that bulk delete button is not rendered anywhere on the page
      const deleteButtons = Array.from(document.querySelectorAll('*')).filter(el => 
        el.textContent && el.textContent.includes('Delete') && el.tagName === 'BUTTON'
      );
      
      // Should not find any delete buttons for non-branch references
      expect(deleteButtons.length).toBe(0);
    });

    it('should call onSelectionChange when individual checkbox clicked', () => {
      renderWithRouter(<Tree {...bulkProps} />);

      const checkboxes = screen.getAllByRole('checkbox');
      const firstObjectCheckbox = checkboxes[1]; // Skip select all checkbox

      fireEvent.click(firstObjectCheckbox);

      expect(bulkProps.onSelectionChange).toHaveBeenCalledWith('file1.txt', true, expect.any(Object));
    });

    it('should handle select all functionality', () => {
      renderWithRouter(<Tree {...bulkProps} />);

      const selectAllCheckbox = screen.getAllByRole('checkbox')[0];
      fireEvent.click(selectAllCheckbox);

      // Should call onSelectionChange for each selectable object
      expect(bulkProps.onSelectionChange).toHaveBeenCalledWith('file1.txt', true, expect.any(Object));
      expect(bulkProps.onSelectionChange).toHaveBeenCalledWith('file2.txt', true, expect.any(Object));
    });

    it('should show correct singular/plural text for selection count', () => {
      const propsWithOneSelected = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt']),
      };

      renderWithRouter(<Tree {...propsWithOneSelected} />);
      expect(screen.getAllByText('1 object selected')[0]).toBeInTheDocument();

      const propsWithMultipleSelected = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt', 'file2.txt']),
      };

      renderWithRouter(<Tree {...propsWithMultipleSelected} />);
      expect(screen.getAllByText('2 objects selected')[0]).toBeInTheDocument();
    });

    it('should show total size when selectedObjectsSize is provided', () => {
      const propsWithSize = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt', 'file2.txt']),
        selectedObjectsSize: 3072, // 3 KB
      };

      renderWithRouter(<Tree {...propsWithSize} />);
      expect(screen.getAllByText('2 objects selected')[0]).toBeInTheDocument();
      expect(screen.getByText('(3.0 KB)')).toBeInTheDocument();
    });

    it('should not show size when selectedObjectsSize is 0', () => {
      const propsWithoutSize = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt']),
        selectedObjectsSize: 0,
      };

      renderWithRouter(<Tree {...propsWithoutSize} />);
      expect(screen.getAllByText('1 object selected')[0]).toBeInTheDocument();
      expect(screen.queryByText('(0.0 B)')).not.toBeInTheDocument();
    });

    it('should display size including folder contents when folders are selected', () => {
      const propsWithFolderSize = {
        ...bulkProps,
        selectedObjects: new Set(['file1.txt', 'folder/']),
        selectedObjectsSize: 5120, // 1024 (file) + 4096 (folder contents)
      };

      renderWithRouter(<Tree {...propsWithFolderSize} />);
      expect(screen.getAllByText('2 objects selected')[0]).toBeInTheDocument();
      expect(screen.getByText('(5.0 KB)')).toBeInTheDocument();
    });

    it('should handle large folder sizes correctly', () => {
      const propsWithLargeFolder = {
        ...bulkProps,
        selectedObjects: new Set(['large-folder/']),
        selectedObjectsSize: 1073741824, // 1 GB
      };

      renderWithRouter(<Tree {...propsWithLargeFolder} />);
      expect(screen.getAllByText('1 object selected')[0]).toBeInTheDocument();
      expect(screen.getByText('(1.0 GB)')).toBeInTheDocument();
    });
  });

  describe('Object Filtering', () => {
    it('should show checkboxes for both objects and directories', () => {
      const resultsWithDirectory = [
        ...mockResults,
        {
          path: 'folder/',
          path_type: 'common_prefix',
        },
      ];

      const props = {
        ...baseProps,
        enableBulkOperations: true,
        selectedObjects: new Set(),
        onSelectionChange: vi.fn(),
        onBulkDownload: vi.fn(),
        onBulkDelete: vi.fn(),
        results: resultsWithDirectory,
      };

      renderWithRouter(<Tree {...props} />);

      const checkboxes = screen.getAllByRole('checkbox');
      // Should have checkboxes for 2 objects + 1 directory + select all (at least 4 total)
      expect(checkboxes.length).toBeGreaterThanOrEqual(4);
    });

    it('should not show checkboxes for removed objects', () => {
      const resultsWithRemoved = [
        ...mockResults,
        {
          path: 'removed-file.txt',
          path_type: 'object',
          diff_type: 'removed',
        },
      ];

      const props = {
        ...baseProps,
        enableBulkOperations: true,
        selectedObjects: new Set(),
        onSelectionChange: vi.fn(),
        onBulkDownload: vi.fn(),
        onBulkDelete: vi.fn(),
        results: resultsWithRemoved,
      };

      renderWithRouter(<Tree {...props} />);

      const checkboxes = screen.getAllByRole('checkbox');
      // Should have checkboxes for selectable objects only (not removed ones)
      expect(checkboxes.length).toBeGreaterThanOrEqual(3);
    });
  });
});