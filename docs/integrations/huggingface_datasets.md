---
title: HuggingFace Datasets
description: Read, write and version your HuggingFace datasets with lakeFS
parent: Integrations

---
# Versioning HuggingFace Datasets with lakeFS


{% include toc_2-3.html %}


[HuggingFace 🤗 Datasets](https://huggingface.co/docs/datasets) is a library for easily accessing and sharing datasets for Audio, Computer Vision, and Natural Language Processing (NLP) tasks.

🤗 Datasets supports access to [cloud storage](https://huggingface.co/docs/datasets/en/filesystems) providers through [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) FileSystem implementations.

[lakefs-spec](https://lakefs-spec.org/) is a community implementation of an fsspec Filesystem that fully leverages lakeFS' capabilities. Let's start by installing it:

## Installation

```shell
pip install lakefs-spec
```

## Configuration

If you've already configured the lakeFS python SDK and/or lakectl, you should have a `$HOME/.lakectl.yaml` file that contains your access credentials and endpoint for your lakeFS environment.

Otherwise, install [`lakectl`](../reference/cli.html##installing-lakectl-locally) and run `lakectl config` to set up your access credentials.


## Reading a Dataset

To read a dataset, all we have to do is use a `lakefs://...` URI when calling [`load_dataset`](https://huggingface.co/docs/datasets/en/loading):

```python
>>> from datasets import load_dataset
>>> 
>>> dataset = load_dataset('csv', data_files='lakefs://example-repository/my-branch/data/example.csv')
```

That's it! this should automatically load the lakefs-spec implementation that we've installed, which will use the `$HOME/.lakectl.yaml` file to read its credentials, so we don't need to pass additional configuration.

## Saving/Loading

Once we've loaded a Dataset, we can save it using the `save_to_disk` method as normal:

```python
>>> dataset.save_to_disk('lakefs://example-repository/my-branch/datasets/example/')
```

At this point, we might want to commit that change to lakeFS, and tag it, so we could share it with our colleagues.

We can do it through the UI or lakectl, but let's do it with the [lakeFS Python SDK](./python.md#using-the-lakefs-sdk):


```python
>>> import lakefs
>>>
>>> repo = lakefs.repository('example-repository')
>>> commit = repo.branch('my-branch').commit(
...     'saved my first huggingface Dataset!',
...     metadata={'using': '🤗'})
>>> repo.tag('alice_experiment1').create(commit)
```

Now, others on our team can load our exact dataset by using the tag we created:

```python
>>> from datasets import load_from_disk
>>>
>>> dataset = load_from_disk('lakefs://example-repository/alice_experiment1/datasets/example/')
```
