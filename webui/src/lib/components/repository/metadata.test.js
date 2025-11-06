import { describe, it, expect } from 'vitest';
import { filterEmptyMetadataFields } from './metadata';

describe('filterEmptyMetadataFields', () => {
    it('should include fields with both key and value', () => {
        const input = [
            { key: 'key1', value: 'value1' },
            { key: 'key2', value: 'value2' },
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(input);
    });

    it('should include fields with non-empty key and empty value', () => {
        const input = [
            { key: 'key_with_empty_value', value: '' },
            { key: 'tag', value: 'v1.0' }
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(input);
    });

    it('should include fields with empty key and non-empty value', () => {
        const input = [
            { key: '', value: 'value for empty key' },
            { key: 'tag', value: 'v1.0' }
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(input);
    });

    it('should filter out fields where both key and value are empty', () => {
        const input = [
            { key: 'key1', value: 'value1' },
            { key: '', value: '' },
            { key: 'key2', value: 'value2' },
        ];
        const expected = [
            { key: 'key1', value: 'value1' },
            { key: 'key2', value: 'value2' },
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(expected);
    });

    it('should filter out fields where both key and value are whitespace only', () => {
        const input = [
            { key: 'key1', value: 'value1' },
            { key: '   ', value: '  ' },
            { key: '\t', value: '\n' },
            { key: 'key2', value: 'value2' },
        ];
        const expected = [
            { key: 'key1', value: 'value1' },
            { key: 'key2', value: 'value2' },
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(expected);
    });

    it('should handle empty array', () => {
        const input = [];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual([]);
    });

    it('should handle array with only empty fields', () => {
        const input = [
            { key: '', value: '' },
            { key: '  ', value: '  ' }
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual([]);
    });

    it('should include fields with whitespace in key but non-empty value', () => {
        const input = [
            { key: '   ', value: 'has value' }
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(input);
    });

    it('should include fields with non-empty key but whitespace in value', () => {
        const input = [
            { key: 'haskey', value: '   ' }
        ];
        const result = filterEmptyMetadataFields(input);
        expect(result).toEqual(input);
    });
});
