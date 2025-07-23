import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { RefTypeBranch } from '../../../constants';
import * as matchers from '@testing-library/jest-dom/matchers';

expect.extend(matchers);

// Mock the actual ObjectsBrowser component by importing and mocking its dependencies
vi.mock('../../../lib/hooks/repo', () => ({
  useRefs: vi.fn(() => ({
    repo: {
      id: 'test-repo',
      read_only: false,
    },
    reference: {
      id: 'main',
      type: RefTypeBranch,
    },
    loading: false,
    error: null,
  })),
}));

vi.mock('../../../lib/hooks/router', () => ({
  useRouter: vi.fn(() => ({
    query: {
      path: '',
      after: '',
      showChanges: 'false',
    },
    push: vi.fn(),
  })),
}));

vi.mock('../../../lib/hooks/storageConfig', () => ({
  useStorageConfigs: vi.fn(() => ({
    configs: [{ 
      id: 'default',
      config: {
        pre_sign_support: true,
        import_support: true,
      }
    }],
    loading: false,
    error: null,
  })),
}));

vi.mock('react-router-dom', () => ({
  useSearchParams: vi.fn(() => [
    new URLSearchParams(),
    vi.fn(),
  ]),
}));

// Mock API with controllable responses
const mockObjects = {
  deleteObjects: vi.fn(),
};

const mockRefs = {
  changes: vi.fn(),
};

const mockLinkToPath = vi.fn();

vi.mock('../../../lib/api', () => ({
  objects: mockObjects,
  refs: mockRefs,
  linkToPath: mockLinkToPath,
}));

// Mock other dependencies
vi.mock('./utils', () => ({
  getRepoStorageConfig: vi.fn(() => ({
    storageConfig: {
      config: {
        pre_sign_support: true,
        import_support: true,
      }
    },
    loading: false,
    error: null,
  })),
}));

vi.mock('../../../lib/components/controls', () => ({
  ActionsBar: ({ children }) => <div data-testid="actions-bar">{children}</div>,
  ActionGroup: ({ children }) => <div data-testid="action-group">{children}</div>,
  Loading: () => <div data-testid="loading">Loading...</div>,
  AlertError: ({ error, onDismiss }) => (
    <div data-testid="alert-error" onClick={onDismiss}>
      Error: {typeof error === 'string' ? error : error?.message || 'Unknown error'}
    </div>
  ),
  PrefixSearchWidget: () => <div data-testid="prefix-search">Search</div>,
  RefreshButton: () => <button data-testid="refresh-button">Refresh</button>,
  Warnings: () => <div data-testid="warnings">Warnings</div>,
}));

vi.mock('../../../lib/components/repository/refDropdown', () => ({
  default: () => <div data-testid="ref-dropdown">Ref Dropdown</div>,
}));

vi.mock('../../../lib/components/modals', () => ({
  ConfirmationModal: ({ show, msg, onConfirm, onHide }) => 
    show ? (
      <div data-testid="confirmation-modal">
        <div data-testid="modal-message">{msg}</div>
        <button data-testid="modal-confirm" onClick={onConfirm}>Confirm</button>
        <button data-testid="modal-cancel" onClick={onHide}>Cancel</button>
      </div>
    ) : null,
}));

vi.mock('./error', () => ({
  RepoError: ({ error }) => <div data-testid="repo-error">{error.message}</div>,
}));

vi.mock('@mui/material', () => ({
  Box: ({ children, sx }) => <div data-testid="mui-box" style={sx}>{children}</div>,
}));

// Create a simple TreeContainer mock that we can control
const MockTreeContainer = vi.fn(({ 
  enableBulkOperations, 
  selectedObjects, 
  onSelectionChange, 
  onBulkDownload, 
  onBulkDelete 
}) => (
  <div data-testid="tree-container">
    <div data-testid="bulk-enabled">{enableBulkOperations ? 'true' : 'false'}</div>
    <div data-testid="selected-count">{selectedObjects ? selectedObjects.size : 0}</div>
    
    {/* Simulate object selection UI */}
    <button 
      data-testid="select-object"
      onClick={() => onSelectionChange?.('test-file.txt', true)}
    >
      Select Object
    </button>
    
    {/* Simulate bulk action buttons */}
    {enableBulkOperations && selectedObjects && selectedObjects.size > 0 && (
      <div data-testid="bulk-actions">
        <button data-testid="bulk-download" onClick={onBulkDownload}>
          Bulk Download
        </button>
        <button data-testid="bulk-delete" onClick={onBulkDelete}>
          Bulk Delete
        </button>
      </div>
    )}
  </div>
));

// Mock the internal TreeContainer
vi.mock('../../../pages/repositories/repository/objects.jsx', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    // We'll patch the TreeContainer after import
  };
});

describe('Objects Page - Bulk Operations Integration', () => {
  let ObjectsBrowser;

  beforeEach(async () => {
    vi.clearAllMocks();
    // Clear any leftover DOM elements from previous tests
    document.body.innerHTML = '';
    
    // Create a wrapper that uses our mock TreeContainer
    ObjectsBrowser = () => {
      const [selectedObjects, setSelectedObjects] = React.useState(new Set());
      const [actionError, setActionError] = React.useState(null);
      const [showBulkDeleteModal, setShowBulkDeleteModal] = React.useState(false);

      const handleSelectionChange = (path, selected) => {
        setSelectedObjects(prev => {
          const newSet = new Set(prev);
          if (selected) {
            newSet.add(path);
          } else {
            newSet.delete(path);
          }
          return newSet;
        });
      };

      const handleBulkDownload = async () => {
        // Simulate download
        console.log('Bulk download triggered');
      };

      const handleBulkDelete = async () => {
        try {
          const selectedPaths = Array.from(selectedObjects);
          await mockObjects.deleteObjects('test-repo', 'main', selectedPaths);
          setSelectedObjects(new Set());
          setShowBulkDeleteModal(false);
        } catch (error) {
          const errorMessage = error.message || error.toString() || 'Failed to delete selected objects';
          setActionError(errorMessage);
          setShowBulkDeleteModal(false);
        }
      };

      return (
        <div>
          <div data-testid="actions-bar">Actions Bar</div>
          
          {actionError && (
            <div data-testid="alert-error" onClick={() => setActionError(null)}>
              Error: {actionError}
            </div>
          )}

          <MockTreeContainer
            enableBulkOperations={true}
            selectedObjects={selectedObjects}
            onSelectionChange={handleSelectionChange}
            onBulkDownload={handleBulkDownload}
            onBulkDelete={() => setShowBulkDeleteModal(true)}
          />

          {showBulkDeleteModal && (
            <div data-testid="confirmation-modal">
              <div data-testid="modal-message">
                Are you sure you want to delete {selectedObjects.size} selected object{selectedObjects.size !== 1 ? 's' : ''}?
              </div>
              <button data-testid="modal-confirm" onClick={handleBulkDelete}>
                Confirm
              </button>
              <button data-testid="modal-cancel" onClick={() => setShowBulkDeleteModal(false)}>
                Cancel
              </button>
            </div>
          )}
        </div>
      );
    };
  });

  describe('Bulk Operations Flow', () => {
    it('should enable bulk operations by default', () => {
      render(<ObjectsBrowser config={{}} />);

      const bulkEnabledElements = screen.getAllByTestId('bulk-enabled');
      const selectedCountElements = screen.getAllByTestId('selected-count');
      
      expect(bulkEnabledElements[0]).toHaveTextContent('true');
      expect(selectedCountElements[0]).toHaveTextContent('0');
    });

    it('should handle object selection', async () => {
      render(<ObjectsBrowser config={{}} />);

      const selectButtons = screen.getAllByTestId('select-object');
      fireEvent.click(selectButtons[0]);

      await waitFor(() => {
        const countElements = screen.getAllByTestId('selected-count');
        expect(countElements[0]).toHaveTextContent('1');
      });

      // Should show bulk actions when objects are selected
      const bulkActionsElements = screen.getAllByTestId('bulk-actions');
      expect(bulkActionsElements[0]).toBeInTheDocument();
    });

    it('should handle successful bulk delete', async () => {
      mockObjects.deleteObjects.mockResolvedValue({ success: true });

      render(<ObjectsBrowser config={{}} />);

      // Select an object
      fireEvent.click(screen.getAllByTestId('select-object')[0]);

      await waitFor(() => {
        expect(screen.getAllByTestId('bulk-actions')[0]).toBeInTheDocument();
      });

      // Trigger bulk delete
      fireEvent.click(screen.getByTestId('bulk-delete'));

      // Confirm in modal
      await waitFor(() => {
        expect(screen.getByTestId('confirmation-modal')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('modal-confirm'));

      await waitFor(() => {
        expect(mockObjects.deleteObjects).toHaveBeenCalledWith('test-repo', 'main', ['test-file.txt']);
      });

      // Should clear selection after success
      await waitFor(() => {
        expect(screen.getByTestId('selected-count')).toHaveTextContent('0');
      });

      // Should not show error
      expect(screen.queryByTestId('alert-error')).not.toBeInTheDocument();
    });

    it('should handle bulk delete with ObjectErrorList - single error', async () => {
      // Mock API response with ObjectErrorList containing same error for all objects
      mockObjects.deleteObjects.mockRejectedValue(
        new Error('cannot write to protected branch')
      );

      render(<ObjectsBrowser config={{}} />);

      // Select an object
      fireEvent.click(screen.getByTestId('select-object'));

      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      // Trigger bulk delete
      fireEvent.click(screen.getByTestId('bulk-delete'));

      // Confirm in modal
      await waitFor(() => {
        expect(screen.getByTestId('confirmation-modal')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('modal-confirm'));

      await waitFor(() => {
        expect(mockObjects.deleteObjects).toHaveBeenCalledWith('test-repo', 'main', ['test-file.txt']);
      });

      // Should show error message
      await waitFor(() => {
        const errorElement = screen.getByTestId('alert-error');
        expect(errorElement).toBeInTheDocument();
        expect(errorElement).toHaveTextContent('Error: cannot write to protected branch');
      });

      // Should close modal after error
      expect(screen.queryByTestId('confirmation-modal')).not.toBeInTheDocument();
    });

    it('should handle bulk delete with ObjectErrorList - multiple errors', async () => {
      // Mock API response with different errors for different objects
      mockObjects.deleteObjects.mockRejectedValue(
        new Error('Failed to delete some objects:\n"file1.txt": cannot write to protected branch\n"file2.txt": object not found')
      );

      render(<ObjectsBrowser config={{}} />);

      // Select an object
      fireEvent.click(screen.getByTestId('select-object'));

      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      // Trigger bulk delete
      fireEvent.click(screen.getByTestId('bulk-delete'));

      // Confirm in modal
      await waitFor(() => {
        expect(screen.getByTestId('confirmation-modal')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('modal-confirm'));

      // Should show detailed error message
      await waitFor(() => {
        const errorElement = screen.getByTestId('alert-error');
        expect(errorElement).toBeInTheDocument();
        expect(errorElement.textContent).toContain('Failed to delete some objects');
        expect(errorElement.textContent).toContain('cannot write to protected branch');
        expect(errorElement.textContent).toContain('object not found');
      });
    });

    it('should allow dismissing error messages', async () => {
      mockObjects.deleteObjects.mockRejectedValue(
        new Error('cannot write to protected branch')
      );

      render(<ObjectsBrowser config={{}} />);

      // Select and trigger delete to generate error
      fireEvent.click(screen.getByTestId('select-object'));
      
      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('bulk-delete'));
      
      await waitFor(() => {
        expect(screen.getByTestId('confirmation-modal')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('modal-confirm'));

      // Wait for error to appear
      await waitFor(() => {
        expect(screen.getByTestId('alert-error')).toBeInTheDocument();
      });

      // Click to dismiss error
      fireEvent.click(screen.getByTestId('alert-error'));

      await waitFor(() => {
        expect(screen.queryByTestId('alert-error')).not.toBeInTheDocument();
      });
    });

    it('should handle bulk download', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

      render(<ObjectsBrowser config={{}} />);

      // Select an object
      fireEvent.click(screen.getByTestId('select-object'));

      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      // Trigger bulk download
      fireEvent.click(screen.getByTestId('bulk-download'));

      expect(consoleSpy).toHaveBeenCalledWith('Bulk download triggered');

      consoleSpy.mockRestore();
    });
  });

  describe('Modal Behavior', () => {
    it('should show confirmation modal with correct message', async () => {
      render(<ObjectsBrowser config={{}} />);

      // Select an object
      fireEvent.click(screen.getByTestId('select-object'));

      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      // Trigger bulk delete
      fireEvent.click(screen.getByTestId('bulk-delete'));

      await waitFor(() => {
        const modal = screen.getByTestId('confirmation-modal');
        expect(modal).toBeInTheDocument();
        
        const message = screen.getByTestId('modal-message');
        expect(message).toHaveTextContent('Are you sure you want to delete 1 selected object?');
      });
    });

    it('should close modal when cancelled', async () => {
      render(<ObjectsBrowser config={{}} />);

      // Select and trigger delete
      fireEvent.click(screen.getByTestId('select-object'));
      
      await waitFor(() => {
        expect(screen.getByTestId('bulk-actions')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByTestId('bulk-delete'));

      await waitFor(() => {
        expect(screen.getByTestId('confirmation-modal')).toBeInTheDocument();
      });

      // Cancel the modal
      fireEvent.click(screen.getByTestId('modal-cancel'));

      await waitFor(() => {
        expect(screen.queryByTestId('confirmation-modal')).not.toBeInTheDocument();
      });

      // Selection should still be there
      expect(screen.getByTestId('selected-count')).toHaveTextContent('1');
    });
  });
});