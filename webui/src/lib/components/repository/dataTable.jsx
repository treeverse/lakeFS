import React from 'react';
import Table from 'react-bootstrap/Table';
import PropTypes from 'prop-types';

/**
 * A reusable data table component with modern styling
 * 
 * @param {Object} props Component props
 * @param {Array} props.columns Array of column definitions with {id, Header, accessor, Cell, className} properties
 * @param {Array} props.data Array of data objects to display in the table
 * @param {boolean} props.bordered Whether to show borders (default: true)
 * @param {boolean} props.hover Whether to show hover effect (default: true)
 * @param {boolean} props.responsive Whether the table should be responsive (default: true)
 * @param {string} props.className Additional CSS class names
 * @returns {JSX.Element} DataTable component
 */
const DataTable = ({
  columns,
  data,
  bordered = true,
  hover = true,
  responsive = true,
  className = '',
}) => {
  if (!data || data.length === 0) {
    return (
      <div className="text-center p-4">
        <p className="text-muted">No data available</p>
      </div>
    );
  }

  return (
    <div className={`object-viewer-sql-results ${className}`}>
      <Table bordered={bordered} hover={hover} responsive={responsive}>
        <thead className="table-dark">
          <tr>
            {columns.map((column) => (
              <th key={column.id || column.Header} className={column.className}>
                <div className="d-flex flex-column">
                  <span>{column.Header}</span>
                  {column.description && <small>{column.description}</small>}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr key={`row-${rowIndex}`}>
              {columns.map((column, colIndex) => {
                const cellValue = column.accessor ? row[column.accessor] : null;
                const cellClass = getCellClassName(cellValue);
                
                return (
                  <td 
                    key={`cell-${rowIndex}-${colIndex}`} 
                    className={`${cellClass} ${column.cellClassName || ''}`}
                  >
                    {column.Cell ? column.Cell({ value: cellValue, row }) : formatCellValue(cellValue)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  );
};

/**
 * Determines the appropriate CSS class based on the data type
 * 
 * @param {any} value The cell value
 * @returns {string} CSS class name
 */
const getCellClassName = (value) => {
  if (typeof value === 'string') return 'string-cell';
  if (typeof value === 'number') return 'number-cell';
  if (value instanceof Date) return 'date-cell';
  return '';
};

/**
 * Formats cell values based on their data type
 * 
 * @param {any} value The cell value
 * @returns {string|number|JSX.Element} Formatted value
 */
const formatCellValue = (value) => {
  if (value === null || value === undefined) return '';
  
  if (typeof value === 'number') {
    return value.toLocaleString('en-US');
  }
  
  if (value instanceof Date) {
    return value.toLocaleString();
  }
  
  return value.toString();
};

DataTable.propTypes = {
  columns: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.string,
      Header: PropTypes.string.isRequired,
      accessor: PropTypes.string,
      Cell: PropTypes.func,
      className: PropTypes.string,
      cellClassName: PropTypes.string,
      description: PropTypes.string,
    })
  ).isRequired,
  data: PropTypes.array.isRequired,
  bordered: PropTypes.bool,
  hover: PropTypes.bool,
  responsive: PropTypes.bool,
  className: PropTypes.string,
};

export default DataTable;
