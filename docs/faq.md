---
layout: default
title: FAQ
description: Frequently Asked Questions (FAQ). Have a question about lakeFS? Find our what others where asking
nav_order: 60
has_children: false
---

# FAQ

### 1. What open source license are you using?
lakeFS is completely free and open source and licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0){:target="_blank"} License.

### 2. The project is backed by a commercial company. What is your commitment to open source?
Since it is important to us to align expectations, we have gathered our thoughts on the subject [here](https://lakefs.io/blog){:target="_blank"}.

### 3. How do I contribute to lakeFS?
Thank you for your interest in contributing to our project. We are excited to build a community of users and contributors around lakeFS. We trust the common wisdom of the community to evolve lakeFS to the best platform it can possibly be. Don't hesitate to get started. Check out our [contribution guide](contributing.md)

### 4. How do I get support for my lakeFS installation?
We are extremely responsive on our slack channel, and we make sure to prioritize and with the community the issues most urgent for it. For SLA based support, please contact us at [support@treeverse,io](mailto:support@lakefs.io).

### 5. Can I get lakeFS as SAAS?
We’d be happy to speak and learn more about your requirements. Email us at [support@treeverse.io](mailto:support@treeverse.io).

### 6. Do you collect data from your active installations?
We collect anonymous usage statistics in order to understand the patterns of use and to detect product gaps we may have so we can fix them.

This is completely optional and may be turned off by setting `stats.enabled` to `false`. See the [configuration reference](reference/configuration.md#reference) for more details.

The data we gather is limited to the following:
1. A UUID which is generated when setting up lakeFS for the first time and contains no personal or otherwise identifiable information
1. The lakeFS version currently running
1. The OS and architecture lakeFS is running on
1. Metadata regarding the database used (version, installed extensions and parameters such as DB Timezone and work memory)
1. Periodic aggregated action counters (e.g. how many "get_object" operations occurred).

### 7. What is the difference between lakeFS / Delta Lake /  Hudi / Iceberg?
We support different use-cases. Delta Lake, Hudi and Iceberg all define dedicated, structured data formats that allow deletes and upserts. lakeFS is format-agnostic and enables consistent cross-collection versioning of your data using git-like operations.  Read our [blog](https://lakefs.io/2020/08/10/data-versioning/) for a more detailed comparison. 

### 8. What inspired your logo?
The [Axolotl](https://en.wikipedia.org/wiki/Axolotl){:target="_blank"} – a species of salamander, also known as the Mexican Lake Monster or the Peter Pan of the animal kingdom. It's a magical creature, living in a lake, just like us :-).

![Axolotl](https://upload.wikimedia.org/wikipedia/commons/f/f6/AxolotlBE.jpg)

<small>
    [copyright](https://en.wikipedia.org/wiki/Axolotl#/media/File:AxolotlBE.jpg)
</small>
