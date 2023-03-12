import { remark } from "remark";
import { beforeEach, describe, test, expect, vi, afterEach } from "vitest";
import { objects } from "../api";

import imageUriReplacer from "./imageUriReplacer";

const TEST_REPO = "test-repo";
const TEST_BRANCH = "test-branch";
const TEST_FILE_NAME = "image.png";
const ADDITIONAL_PATH = "additional/path";

vi.mock("../api", () => {
  const mockObjects = {
    objects: {
      getPresignedUrl: vi.fn(),
    },
  };
  return mockObjects;
});

describe("imageUriReplacer", async () => {
  let mockObjects: any;

  beforeEach(() => {
    mockObjects = { objects };
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  test("Basic replacement", async () => {
    mockObjects.getPresignedUrl.mockImplementation(
      async (repo: string, branch: string, path: string) => {
        expect(repo).toEqual(TEST_REPO);
        expect(branch).toEqual(TEST_BRANCH);
        return `https://www.example.com/${path}`;
      }
    );
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](lakefs://${TEST_REPO}/${TEST_BRANCH}/${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](https://www.example.com/${TEST_FILE_NAME})
`;

    const result = await remark().use(imageUriReplacer).process(markdown);
    expect(mockObjects.getPresignedUrl).toHaveBeenCalledTimes(1);
    expect(mockObjects.getPresignedUrl).toHaveBeenCalledWith(
      TEST_REPO,
      TEST_BRANCH,
      TEST_FILE_NAME
    );
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });

  test("Replacement with additional path", async () => {
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](lakefs://${TEST_REPO}/${TEST_BRANCH}/${ADDITIONAL_PATH}/${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](https://www.example.com/${ADDITIONAL_PATH}/${TEST_FILE_NAME})
`;

    const result = await remark().use(imageUriReplacer).process(markdown);
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });

  test("Renders original URI if API throws", async () => {
    // hide the error in the console
    vi.spyOn(console, "error").mockImplementation(() => {
      //noop
    });
    mockObjects.getPresignedUrl.mockImplementation(async () => {
      throw new Error("API error");
    });
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](lakefs://${TEST_REPO}/${TEST_BRANCH}/${ADDITIONAL_PATH}/${TEST_FILE_NAME})
`;

    const result = await remark().use(imageUriReplacer).process(markdown);
    expect(mockObjects.getPresignedUrl).toHaveBeenCalledTimes(1);
    expect(result.toString()).toEqual(markdown);
  });
});
