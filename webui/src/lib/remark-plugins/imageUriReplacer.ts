import { visit } from "unist-util-visit";
import type { Node } from "unist";
import type { Root } from "mdast";
import type { Plugin } from "unified";

type ImageUriReplacerOptions = {
  repo: string;
  ref: string;
  path: string;
};

const ABSOLUTE_URL_REGEX = /^(https?):\/\/.*/;
const qs = (queryParts: { [key: string]: string }) => {
  const parts = Object.keys(queryParts).map((key) => [key, queryParts[key]]);
  return new URLSearchParams(parts).toString();
};
export const getImageUrl = (
  repo: string,
  ref: string,
  path: string
): string => {
  const query = qs({ path });
  return `/api/v1/repositories/${encodeURIComponent(
    repo
  )}/refs/${encodeURIComponent(ref)}/objects?${query}`;
};

const imageUriReplacer: Plugin<[ImageUriReplacerOptions], Root> =
  (options) => (tree) => {
    visit(tree, "image", (node: Node & { url: string }) => {
      if (node.url.startsWith("lakefs://")) {
        const [repo, ref, ...imgPath] = node.url.split("/").slice(2);
        node.url = getImageUrl(repo, ref, imgPath.join("/"));
      } else if (!node.url.match(ABSOLUTE_URL_REGEX)) {
        // If the image is not an absolute URL, we assume it's a relative path
        // relative to repo and ref
        if (node.url.startsWith("/")) {
          node.url = node.url.slice(1);
        }
        // relative to MD file location
        if (node.url.startsWith("./")) {
          node.url = `${options.path.split("/").slice(0, -1)}/${node.url.slice(
            2
          )}`;
        }
        node.url = getImageUrl(options.repo, options.ref, node.url);
      }
    });
  };

export default imageUriReplacer;
