---
layout: default
title: FAQ
description: Frequently Asked Questions (FAQ). Have a question about lakeFS? Find our what others where asking
nav_order: 60
has_children: false
---

# FAQ

### 1. Is lakeFS open source?
lakeFS is completely free and open source and licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) License. We maintain a public [product roadmap](https://docs.lakefs.io/understand/roadmap.html) and [Slack channel](https://lakefs.io/slack) for open discussions.

### 2. How does lakeFS data versioning work?
lakeFS uses a copy-on-write mechanism to avoid data duplication. For example, creating a new branch is a metadata-only operation: no objects are actually copied. Only when an object changes does lakeFS create another version of the data in the storage. For more information, see [Data Model](https://docs.lakefs.io/understand/data-model.html).

### 3. How do I get support for my lakeFS installation?
We are extremely responsive on our slack channel, and we make sure to prioritize and with the community the issues most urgent for it. For SLA based support, please contact us at [support@treeverse.io](mailto:support@treeverse.io).

### 4. Do you collect data from your active installations?
We collect anonymous usage statistics in order to understand the patterns of use and to detect product gaps we may have so we can fix them. This is completely optional and may be turned off by setting `stats.enabled` to `false`. See the [configuration reference](reference/configuration.md#reference) for more details.

### 5. How is lakeFS different from Delta Lake / Hudi / Iceberg?
Delta Lake, Hudi and Iceberg all define dedicated, structured data formats that allow deletes and upserts. lakeFS is format-agnostic and enables consistent cross-collection versioning of your data using git-like operations. Read our [blog](https://lakefs.io/hudi-iceberg-and-delta-lake-data-lake-table-formats-compared/) for a more detailed comparison. 

### 6. What inspired the lakeFS logo?
The [Axolotl](https://en.wikipedia.org/wiki/Axolotl){:target="_blank"} – a species of salamander, also known as the Mexican Lake Monster or the Peter Pan of the animal kingdom. It's a magical creature, living in a lake, just like us :-).

![Axolotl](https://upload.wikimedia.org/wikipedia/commons/f/f6/AxolotlBE.jpg)

<small>
    [copyright](https://en.wikipedia.org/wiki/Axolotl#/media/File:AxolotlBE.jpg)
</small>
