import { describe, it, expect } from 'vitest';
import {destinationPath} from "./objects";

describe('destinationPath', () => {
    it('should use full override path for single file', () => {
        const file = { path: "file1.txt" };
        const path = "override/path.txt";
        const result = destinationPath(path, file, false);
        expect(result).toEqual("override/path.txt");
    });

    it('should treat override path as folder prefix when multiple files', () => {
        const file = { path: "folder/file1.txt" };
        const path = "override/";
        const result = destinationPath(path, file, true);
        expect(result).toEqual("override/folder/file1.txt");
    });

    it('should clean windows path and trim slashes', () => {
        const file = { path: "\\nested\\file1.txt" };
        const path = "override///";
        const result = destinationPath(path, file, true);
        expect(result).toEqual("override/nested/file1.txt");
    });

    it('should fallback to file.path if no override path provided', () => {
        const file = { path: "some/path.txt" };
        const path = "";
        const result = destinationPath(path, file, false);
        expect(result).toEqual("some/path.txt");
    });

    it("should normalize multiple slashes in override path for multiple files", () => {
        const file = { path: "file1.txt" };
        const result = destinationPath("override///aaa", file, true);
        expect(result).toEqual("override/aaa/file1.txt");
    });

    it("should remove all leading slashes from file.path when constructing full path", () => {
        const file = { path: "/nested/file1.txt" };
        const result = destinationPath("override", file, true);
        expect(result).toEqual("override/nested/file1.txt");
    });

    it("should remove all leading slashes from path override", () => {
        const file = { path: "file1.txt" };
        const result = destinationPath("//override", file, true);
        expect(result).toEqual("override/file1.txt");
    });
});
