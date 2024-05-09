import { visit } from "unist-util-visit";
import type { Node } from "unist";
import {objects} from "../api";

type ImageUriReplacerOptions = {
  repo: string;
  ref: string;
  path: string;
  presign: boolean;
};

const ABSOLUTE_URL_REGEX = /^(https?):\/\/.*/;
const qs = (queryParts: { [key: string]: string }) => {
  const parts = Object.keys(queryParts).map((key) => [key, queryParts[key]]);
  return new URLSearchParams(parts).toString();
};
export const getImageUrl = async (
  repo: string,
  ref: string,
  path: string,
  presign: boolean,
): Promise<string> => {
  if (presign) {
    try {
      const obj = await objects.getStat(repo, ref, path, true);
      return obj.physical_address;
    } catch(e) {
      console.error("failed to fetch presigned URL", e);
      return ""
    }
  }

  const query = qs({ path });
  return `/api/v1/repositories/${encodeURIComponent(
    repo
  )}/refs/${encodeURIComponent(ref)}/objects?${query}`;
};

const imageUriReplacer =
  (options: ImageUriReplacerOptions) => async (tree: Node) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const promises: any[] = [];
    visit(tree, "image", visitor);
    await Promise.all(promises);

    function visitor(node: Node & { url: string }) {
      if (node.url.startsWith("lakefs://")) {
        const [repo, ref, ...imgPath] = node.url.split("/").slice(2);
        const p = getImageUrl(repo, ref, imgPath.join("/"), options.presign).then((url) => node.url = url);
        promises.push(p);
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
        const p = getImageUrl(options.repo, options.ref, node.url, options.presign).then((url) => node.url = url);
        promises.push(p);
      }
    }
  };

export default imageUriReplacer;
