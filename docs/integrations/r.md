---
layout: default
title: Using R with lakeFS
description: How to use lakeFS from R including creating branches, committing changes, and merging.
parent: Integrations
nav_order: 60
has_children: false
---

# Using R with lakeFS

R is a powerful language used widely in data science. lakeFS interfaces with R in two ways: 

* To **read and write data in lakeFS** use standard S3 tools such as the `aws.s3` library. lakeFS has a [S3 gateway](https://docs.lakefs.io/understand/architecture.html#s3-gateway) which presents a lakeFS repository as an S3 bucket. 
* For working with **lakeFS operations such as branches and commits** use the [API](https://docs.lakefs.io/reference/api.html) for which can be accessed from R using the `httr` library. 

_To see examples of R in action with lakeFS please visit the [lakeFS-samples](https://github.com/treeverse/lakeFS-samples/) repository and the [sample](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/R.ipynb) [notebooks](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/R-weather.ipynb)_.

{% include toc.html %}

## Reading and Writing from lakeFS with R

Working with data stored in lakeFS from R is the same as you would with an S3 bucket, via the [S3 Gateway that lakeFS provides](https://docs.lakefs.io/understand/architecture.html#s3-gateway).

You can use any library that interfaces with S3. In this example we'll use the [aws.s3](https://github.com/cloudyr/aws.s3) library.

```r
install.packages(c("aws.s3"))
library(aws.s3)
```

### Configuration 

The [R S3 client documentation](https://cloud.r-project.org/web/packages/aws.s3/aws.s3.pdf) includes full details of the configuration options available. A good approach for using it with lakeFS set the endpoint and authentication details as environment variables: 

```r
Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAIOSFODNN7EXAMPLE",
           "AWS_SECRET_ACCESS_KEY" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
           "AWS_S3_ENDPOINT" = "lakefs.mycorp.com:8000")
```

_Note: it is generally best practice to set these environment variables outside of the R script; it is done so here for convenience of the example._

In conjunction with this you must also specify `region` and `use_https` _in each call of an `aws.s3` function_ as these cannot be set globally. For example: 

```r
bucketlist(
    region = "",
    use_https = FALSE
    )
```

* `region` should always be empty
* `use_https` should be set to `TRUE` or `FALSE` depending on whether your lakeFS endpoint uses HTTPS.

### Listing repositories

The S3 gateway exposes a repository as a bucket, and so using the `aws.s3` function `bucketlist` will return a list of available repositories on lakeFS: 

```r
bucketlist(
    region = "",
    use_https = FALSE
    )
```

### Writing to lakeFS from R

Assuming you're using the `aws.s3` library there various functions available including `s3save`, `s3saveRDS`, and `put_object`. Here's an example of writing an R object to lakeFS: 

```r
repo_name <- "example"
branch <- "development"

s3saveRDS(x=my_df, 
          bucket = repo_name, 
          object = paste0(branch,"/my_df.R"), 
          region = "",
          use_https = FALSE)
```

You can also upload local files to lakeFS using R and the `put_object` function: 

```r
repo_name <- "example"
branch <- "development"
local_file <- "/tmp/never.gonna"

put_object(file = local_file, 
           bucket = repo_name, 
           object = paste0(branch,"/give/you/up"),
           region = "",
           use_https = FALSE)
```

### Reading from lakeFS with R

As with writing data from R to lakeFS, there is a similar set of functions for reading data. These include `s3load`, `s3readRDS`, and `get_object`. Here's an example of reading an R object from lakeFS: 

```r
repo_name <- "example"
branch <- "development"

my_df <- s3readRDS(bucket = repo_name, 
                   object = paste0(branch,"/my_data.R"),
                   region = "",
                   use_https = FALSE)
```

### Listing Objects

In general you should always specify a branch prefix when listing objects. Here's an example to list the `main` branch in the `quickstart` repository: 

```R
get_bucket_df(bucket = "quickstart",
              prefix = "main/",
              region = "",
              use_https = FALSE)
```

When listing objects in lakeFS there is a special case which is the repository/bucket level. When you list at this level you will get the branches returned as folders. These are not listed recursively, unless you list something under the branch. To understand more about this please refer to [#5441](https://github.com/treeverse/lakeFS/issues/5441)

### Working with Arrow

Arrow's [R library](https://arrow.apache.org/docs/r/index.html) includes [powerful support](https://arrow.apache.org/docs/r/index.html#what-can-the-arrow-package-do) for data analysis, including reading and writing multiple file formats including Parquet, Arrow, CSV, and JSON. It has functionality for [connecting to S3](https://arrow.apache.org/docs/r/articles/fs.html), and thus integrates perfectly with lakeFS. 

To start with install and load the library

```r
install.packages("arrow")
library(arrow)
```

Then create an S3FileSystem object to connect to your lakeFS instance

```r
lakefs <- S3FileSystem$create(
    endpoint_override = "lakefs.mycorp.com:8000",
    scheme = "http"
    access_key = "AKIAIOSFODNN7EXAMPLE", 
    secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 
    region = "",
)
```

From here you can list the contents of a particular lakeFS repository and branch

```r
lakefs$ls(path = "quickstart/main")
```

To read a Parquet from lakeFS with R use the `read_parquet` function

```r
lakes <- read_parquet(lakefs$path("quickstart/main/lakes.parquet"))
```

Writing a file follows a similar pattern. Here is rewriting the same file as above but in Arrow format

```r
write_feather(x = lakes,
              sink = lakefs$path("quickstart/main/lakes.arrow"))
```

## Performing lakeFS Operations using the lakeFS API from R

As well as reading and writing data, you will also want to carry out lakeFS operations from R including creating branches, committing data, and more. 

To do this call the lakeFS [API](https://docs.lakefs.io/reference/api.html) from the `httr` library. You should refer to the API documentation for full details of the endpoints and their behaviour. Below are a few examples to illustrate the usage. 

### Check the lakeFS Server Version

This is a useful API call to establish connectivity and test authentication. 

```r
library(httr)
lakefs_api_url <- "lakefs.mycorp.com:8000/api/v1"
lakefsAccessKey <- "AKIAIOSFODNN7EXAMPLE"
lakefsSecretKey <- "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

r=GET(url=paste0(lakefs_api_url, "/config/version"), 
      authenticate(lakefsAccessKey, lakefsSecretKey))
```

The returned object `r` can be inspected to determine the outcome of the operation by comparing it to the status codes specified in the API. Here is some example R code to demonstrate the idea: 

```r
if (r$status_code == 200) {
    print(paste0("✅lakeFS credentials and connectivity verified. ℹ️lakeFS version ",content(r)$version))   
} else {
    print("🛑 failed to get lakeFS version")
    print(content(r)$message)
}
```

### Create a Repository

```r
library(httr)
lakefs_api_url <- "lakefs.mycorp.com:8000/api/v1"
lakefsAccessKey <- "AKIAIOSFODNN7EXAMPLE"
lakefsSecretKey <- "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
repo_name <- "my_new_repo"

# Define the payload
body=list(name=repo_name, 
          storage_namespace="s3://example-bucket/foo")

# Call the API
r=POST(url=paste0(lakefs_api_url, "/repositories"), 
        authenticate(lakefsAccessKey, lakefsSecretKey),
        body=body, encode="json")
```

### Commit Data

```r
library(httr)
lakefs_api_url <- "lakefs.mycorp.com:8000/api/v1"
lakefsAccessKey <- "AKIAIOSFODNN7EXAMPLE"
lakefsSecretKey <- "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
repo_name <- "my_new_repo"
branch <- "example"

# Define the payload
body=list(message="add some data and charts", 
          metadata=list(
              client="httr", 
              author="rmoff"))

# Call the API
r=POST(url=paste0(lakefs_api_url, "/repositories/", repo_name, "/branches/", branch, "/commits"), 
       authenticate(lakefsAccessKey, lakefsSecretKey),
       body=body, encode="json")
```