import { visit } from "unist-util-visit";
import type { Node } from "unist";
import type { Root } from "mdast";
import type { Plugin } from "unified";

import { objects } from "../api";

const imageUriReplacer: Plugin<[], Root> = () => async (tree) => {
  const images: Array<Node & { url: string }> = [];
  visit(tree, "image", (node: Node & { url: string }) => {
    if (node.url.startsWith("lakefs://")) {
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
