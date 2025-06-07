---
title: FAQ
description: Have a question about lakeFS? Check out this list of Frequently Asked
  Questions
redirect_from:
- "/faq.html"
---

# lakeFS Frequently Asked Questions (FAQ)

### 1. Is lakeFS open-source?
lakeFS is completely free, open-source, and licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) License. We maintain a public [product roadmap][roadmap] and a [Slack channel](https://lakefs.io/slack) for open discussions.

### 2. How does lakeFS data versioning work?
lakeFS uses zero-copy branching to avoid data duplication. That is, creating a new branch is a metadata-only operation: no objects are actually copied. Only when an object changes does lakeFS create another [version of the data](https://lakefs.io/blog/data-versioning/) in the storage. For more information, see [Versioning internals]({% link understand/how/versioning-internals.md %}).

### 3. How do I get support for my lakeFS installation?
We are extremely responsive on our Slack channel, and we make sure to prioritize the most pressing issues for the community. For SLA-based support, please contact us at [support@treeverse.io](mailto:support@treeverse.io).

### 4. Do you collect data from your active installations?
We collect anonymous usage statistics to understand the patterns of use and to detect product gaps we may have so we can fix them. This is optional and may be turned off by setting `stats.enabled` to `false`. See the [configuration reference][config-ref] for more details.


The data we gather is limited to the following:
1. A UUID which is generated when setting up lakeFS for the first time and contains no personal or otherwise identifiable information,
1. The lakeFS version currently running,
1. The OS and architecture lakeFS is running on,
1. Metadata regarding the database used (version, installed extensions and parameters such as DB Timezone and work memory),
1. Periodic aggregated action counters (e.g. how many "get_object" operations occurred).

### 5. How is lakeFS different from Delta Lake / Hudi / Iceberg?
Delta Lake, Apache Hudi, and Apache Iceberg all define dedicated, structured data formats that allow deletes and upserts. lakeFS is format-agnostic and enables consistent cross-collection versioning of your data using Git-like operations. Read our [comparison](https://lakefs.io/blog/hudi-iceberg-and-delta-lake-data-lake-table-formats-compared/) for a more detailed comparison. 

### 6. What inspired the lakeFS logo?
The [Axolotl](https://en.wikipedia.org/wiki/Axolotl){:target="_blank"} – a species of salamander, also known as the Mexican Lake Monster or the Peter Pan of the animal kingdom. It's a magical creature, living in a lake - just like us! :)

![Axolotl](https://upload.wikimedia.org/wikipedia/commons/f/f6/AxolotlBE.jpg)

<small>
    [copyright](https://en.wikipedia.org/wiki/Axolotl#/media/File:AxolotlBE.jpg)
</small>

[config-ref]:  {% link reference/configuration.md %}#reference
[roadmap]:  {% link project/index.md %}#roadmap
