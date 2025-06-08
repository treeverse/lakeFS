---
title: Documentation
description: How to contribute to the lakeFS Documentation, including style guide
---

# lakeFS Documentation

Any contribution to the docs, whether it is in conjunction with a code contribution or as a standalone, is appreciated.

Please see [the contributing guide](/project/contributing.md) for details on contributing to lakeFS in general. 


!!! warning "Notice"
    lakeFS documentation is written using Markdown.  Make sure to familiarize yourself with the [Markdown Guide](https://www.markdownguide.org/basic-syntax/#heading-best-practices).


## lakeFS Documentation Philosophy

We are heavily inspired by the [Diátaxis](https://diataxis.fr/) approach to documentation. 

At a very high-level, it defines documentation as falling into one of four categories: 

- How To
- Tutorial
- Reference
- Explanation

There is a lot more to it than this, and you are encouraged to read the [Diátaxis](https://diataxis.fr/) website for more details.
Its application to lakeFS was discussed in [#6197](https://github.com/treeverse/lakeFS/issues/6197#issuecomment-1645933769)

## lakeFS Style Guide:

* Don't use unnecessary tech jargon or vague/wordy constructions - keep it friendly, not condescending.
* Be inclusive and welcoming - use gender-neutral words and pronouns when talking about abstract people like users and developers.
* Replace complex expressions with simpler ones.
* Keep it short - 25-30 words max per sentence.  Otherwise, your readers might get lost on the way. 
* Use active voice instead of passive. For example: This feature can be used to do task X. vs. You can use this feature to do task X. The second one reads much better, right?
* You can explain things better by including examples. Show, not tell. Use illustrations, images, gifs, code snippets, etc.
* Establish a visual hierarchy to help people quickly find the information they need. Use text formatting to create levels of title and subtitle (such as `#` to `######` markdown headings).  The title of every page should use the topmost heading `#`; all other headings on the page should use lower headers `##` to `######`.

## Headings

The title of the page should be H1 (`#` in markdown). Use headings in descending order and do not skip any. 

## Test your changes locally

If you have the necessary dependencies installed, you can run Jekyll to build and serve the documentation from your machine using the provided Makefile target: 

```sh
make docs-serve
```

## Link Checking locally

When making a pull request to lakeFS that involves a `docs/*` file, a [GitHub action](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/docs-pr.yaml) will automagically check the links. You can also run this link checker manually on your local machine: 

1. Build the site: 

    ```bash
    mkdocs build
    ```

2. Check the links: 

    ```
    docker run --rm \
            --name lakefs_docs_lychee \
            --volume "$PWD:/data"\
            --volume "/tmp:/output"\
            --tty \
            lycheeverse/lychee:master \
            --exclude-file /data/.lycheeignore \
            --output /output/lychee_report.md \
            --format markdown \
            /data/docs/site
    ```

3. Review the `lychee_report.md` in your local `/tmp` folder
 