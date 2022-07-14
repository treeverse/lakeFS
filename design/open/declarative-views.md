# Proposal for declarative scoped repositories


### Goals

Make it easy for users to compose data coming from different sources (lakeFS repositories & references, external object store locations) as input for processing jobs and data science experiments.

Today, simply using tags and attaching job/notebook URI to a lakeFS commit allows for reproducibility, but has a couple of limitations:

1. We explicitly tie all input data to a single repository. Usually the scope of the repository is determined by the writer and not the reader - this means that input data could possibly come from multiple repos and requires tracking the inputs in some other place.
1. The inverse is also true - a given repository could contain a lot more data than what was actually required for a run/experiment/transformation/etc.. To ensure reproducibility in that case, we need to either somehow know what input is actually _used_ by that run, or ensure the lakeFS commit is never garbage collected entirely. Both hard to manage.
1. In some cases, we know a process is deterministic, so all we want to do is map the input and the transformation without actually saving the output. This is especially useful when we run multiple experiments with the same input data and different parameters but storing all outputs is expensive


### Design

Provide a layer similar to a Dockerfile - compose a "virtual repository" or view in a declarative, layered manner.
A set of API endpoints will be used to create and manage these "images".

Here's what a `Lakefile` could look like:

```dockerfile
# not required, but we can use an entire ref as a "base layer"
FROM lakefs://repo1/tag1

# Or implicitly use
FROM scratch

# OR inherit from another image
FROM lakefs://image:v123

# Compose additional inputs
ADD "lakefs://repo2/commit123/path/to/collection/"                 "path/to/collection/"
ADD "lakefs://repo2/commit456/path/to/collection/date=11072022/"   "path/to/collection/date=11072022/"
ADD "lakefs://repo4/tag123/pictures/cats/"                         "pictures/animals/cats/"

# Optionally, if the submitting user has a `fs:ImportFromStorage` permission we can also support:
ADD "s3://my-bucket/some-path/"                                    "external/some-path/"

# Let's also support labels as used by Dockerfiles! this is useful to point to external locations, map dependencies and other metadata
LABEL "author=oz.katz"
LABEL "notebook_url=https://dbc-123.cloud.databricks.com/?o=12345#notebook/45678"
LABEL "spark_version=3.3.2"
LABEL "spark_terraform=https://github.com/org/terraform/tree/c54624/spark/"
```

Along with this format, we also define a set of endpoints:

* `POST /api/v1/images/<image_tag>` - accepts a `Lakefile` and creates a lakeFS "image" for the given image tag specified. Returns the URI and metadata for the image.
* `GET /api/v1/images/<image_tag>` - returns URI and metadata for the given image by its tag
* `GET /api/v1/images` - list images
* `DELETE /api/v1/images/<image_tag>` - delete an image

A set of IAM permissions:

* `fs:ReadImage` - Get information about an image
* `fs:CreateImage` - Create a new image
* `fs:AttachStorageNamespace` - This already exists, but should probably be used since an image should be stored somewhere..
* `fs:ImportFromStorage` - Also exists, could be used if we wish to support mapping external object store paths
* `fs:DeleteImage` - Delete an image


### Open Questions

1. Do we want to support external locations not from lakeFS? This could be achieved (albeit with some more work by the user) by defining a repository that imports an external location and then using that instead.
1. Syntax - does the `Dockerfile` analogy make sense? is there a better/more intuitive way to achieve this declaratively?
1. Using images as input - what does it look like in practice? how are they accessed using the different clients (s3gw, HadoopFS, lakeFS objects API and SDKs...)
1. How do we handle images when doing Garbage collection?
1. How do we handle non-immutable refs? Not just external storage but also branch HEADs and anything based on HEADs (i.e. `main~10`). Warn about it on creation? Disallow?
1. Handling permissions in general - are permissions checked according to the source repository? or do we create a permission layout for a image similar to a repository?
