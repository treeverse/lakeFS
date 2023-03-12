import { visit } from "unist-util-visit";
import type { Node } from "unist";
import type { Root } from "mdast";
import type { Plugin } from "unified";

import { objects } from "../api";

type ImageUriReplacerOptions = {
  repo: string;
  branch: string;
};

const ABSOLUTE_URL_REGEX = /^(https?):\/\/.*/;

const imageUriReplacer: Plugin<[ImageUriReplacerOptions], Root> =
  (options) => async (tree) => {
    const images: Array<Node & { url: string }> = [];
    visit(tree, "image", (node: Node & { url: string }) => {
      if (node.url.startsWith("lakefs://")) {
        images.push(node);
      } else if (!node.url.match(ABSOLUTE_URL_REGEX)) {
        // If the image is not an absolute URL, we assume it's a relative path
        // and we prefix it with the repo and branch.'
        if (node.url.startsWith("/")) {
          node.url = node.url.slice(1);
        }
        node.url = `lakefs://${options.repo}/${options.branch}/${node.url}`;
        images.push(node);
      }
    });

    const promises: Array<Promise<void>> = [];
    for (const image of images) {
      const [repo, branch, ...path] = image.url.split("/").slice(2);
      const promise = objects
        .getPresignedUrlForDownload(repo, branch, path.join("/"))
        .then((res: string) => {
          image.url = res;
        })
        .catch((err: Error) => {
          console.error(err);
        });
      promises.push(promise);
    }

    await Promise.all(promises);
  };

export default imageUriReplacer;
