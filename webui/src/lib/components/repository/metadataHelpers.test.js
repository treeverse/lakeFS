import { describe, it, expect } from "vitest";
import {
    hasInvalidKeys,
    fieldsToMetadata,
    touchInvalidFields,
    getFieldError,
    getMetadataIfValid,
} from "./metadataHelpers";

describe("hasInvalidKeys", () => {
    it("returns false for empty array", () => {
        expect(hasInvalidKeys([])).toBe(false);
    });

    it("returns false when all keys are valid", () => {
        const fields = [
            { key: "key1", value: "value1", touched: false },
            { key: "key2", value: "", touched: false },
        ];

        expect(hasInvalidKeys(fields)).toBe(false);
    });

    it("returns true when any key is empty", () => {
        const fields = [
            { key: "", value: "value", touched: false },
            { key: "valid", value: "value", touched: false },
        ];

        expect(hasInvalidKeys(fields)).toBe(true);
    });

    it("returns true when any key is whitespace only", () => {
        const fields = [
            { key: "   ", value: "value", touched: false },
            { key: "valid", value: "value", touched: false },
        ];

        expect(hasInvalidKeys(fields)).toBe(true);
    });
});

describe("fieldsToMetadata", () => {
    it("converts empty array to empty object", () => {
        expect(fieldsToMetadata([])).toEqual({});
    });

    it("converts fields to key-value object", () => {
        const fields = [
            { key: "env", value: "prod", touched: false },
            { key: "region", value: "us-east-1", touched: true },
        ];

        expect(fieldsToMetadata(fields)).toEqual({
            env: "prod",
            region: "us-east-1",
        });
    });

    it("handles empty values", () => {
        const fields = [{ key: "key1", value: "", touched: false }];

        expect(fieldsToMetadata(fields)).toEqual({ key1: "" });
    });
});

describe("touchInvalidFields", () => {
    it("returns empty array for empty input", () => {
        expect(touchInvalidFields([])).toEqual([]);
    });

    it("marks fields with empty keys as touched", () => {
        const fields = [
            { key: "", value: "value", touched: false },
            { key: "valid", value: "value", touched: false },
        ];

        const result = touchInvalidFields(fields);

        expect(result[0].touched).toBe(true);
        expect(result[1].touched).toBe(false);
    });

    it("marks fields with whitespace keys as touched", () => {
        const fields = [{ key: "   ", value: "value", touched: false }];

        const result = touchInvalidFields(fields);

        expect(result[0].touched).toBe(true);
    });

    it("preserves already touched state", () => {
        const fields = [{ key: "valid", value: "value", touched: true }];

        const result = touchInvalidFields(fields);

        expect(result[0].touched).toBe(true);
    });

    it("does not mutate original array", () => {
        const fields = [{ key: "", value: "value", touched: false }];

        const result = touchInvalidFields(fields);

        expect(fields[0].touched).toBe(false); // Original unchanged
        expect(result[0].touched).toBe(true); // New array modified
    });
});

describe("getFieldError", () => {
    it("returns null for untouched field", () => {
        const field = { key: "", value: "", touched: false };
        expect(getFieldError(field)).toBeNull();
    });

    it("returns error message for empty key when touched", () => {
        const field = { key: "", value: "", touched: true };
        expect(getFieldError(field)).toBe("Key is required");
    });

    it("returns error message for whitespace key when touched", () => {
        const field = { key: "   ", value: "", touched: true };
        expect(getFieldError(field)).toBe("Key is required");
    });

    it("returns null for valid key", () => {
        const field = { key: "valid", value: "", touched: true };
        expect(getFieldError(field)).toBeNull();
    });
});

describe("getMetadataIfValid", () => {
    it("returns metadata object for valid fields", () => {
        const fields = [
            { key: "env", value: "prod", touched: false },
            { key: "region", value: "us-east-1", touched: false },
        ];

        const result = getMetadataIfValid(fields);

        expect(result).toEqual({
            env: "prod",
            region: "us-east-1",
        });
    });

    it("returns null for fields with empty keys", () => {
        const fields = [
            { key: "", value: "value", touched: false },
            { key: "valid", value: "value", touched: false },
        ];

        const result = getMetadataIfValid(fields);

        expect(result).toBeNull();
    });

    it("returns empty object for empty array", () => {
        const result = getMetadataIfValid([]);

        expect(result).toEqual({});
    });

    it("returns null for whitespace-only keys", () => {
        const fields = [{ key: "   ", value: "value", touched: false }];

        const result = getMetadataIfValid(fields);

        expect(result).toBeNull();
    });
});
