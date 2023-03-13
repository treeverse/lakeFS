import { remark } from "remark";
import { describe, test, expect } from "vitest";
import { getImageUrl } from "./imageUriReplacer";

import imageUriReplacer from "./imageUriReplacer";

const TEST_REPO = "test-repo";
const TEST_BRANCH = "test-branch";
const TEST_FILE_NAME = "image.png";
const ADDITIONAL_PATH = "additional/path";

describe("imageUriReplacer", async () => {
  test("Basic replacement", async () => {
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](lakefs://${TEST_REPO}/${TEST_BRANCH}/${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](${getImageUrl(TEST_REPO, TEST_BRANCH, TEST_FILE_NAME)})
`;

    const result = await remark()
      .use(imageUriReplacer, {
        repo: TEST_REPO,
        branch: TEST_BRANCH,
      })
      .process(markdown);
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });

  test("Replacement with additional path", async () => {
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](lakefs://${TEST_REPO}/${TEST_BRANCH}/${ADDITIONAL_PATH}/${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](${getImageUrl(
      TEST_REPO,
      TEST_BRANCH,
      `${ADDITIONAL_PATH}/${TEST_FILE_NAME}`
    )})
`;

    const result = await remark()
      .use([
        imageUriReplacer,
        {
          repo: TEST_REPO,
          branch: TEST_BRANCH,
        },
      ])
      .process(markdown);
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });

  test("Supports relative paths w/o leading slash", async () => {
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](${getImageUrl(TEST_REPO, TEST_BRANCH, TEST_FILE_NAME)})
`;

    const result = await remark()
      .use(imageUriReplacer, {
        repo: TEST_REPO,
        branch: TEST_BRANCH,
      })
      .process(markdown);
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });

  test("Supports relative paths w/ leading slash", async () => {
    const markdown = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](/${TEST_FILE_NAME})
`;

    const markdownWithReplacedImage = `# README

Text and whatever and hey look at this image:
![lakefs://image.png](${getImageUrl(TEST_REPO, TEST_BRANCH, TEST_FILE_NAME)})
`;

    const result = await remark()
      .use(imageUriReplacer, {
        repo: TEST_REPO,
        branch: TEST_BRANCH,
      })
      .process(markdown);
    expect(result.toString()).toEqual(markdownWithReplacedImage);
  });
});
