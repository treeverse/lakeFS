# Template Server

## About

The template server adds a simple generic templating capability to the
lakeFS web server.  It adds server-side support for administrators and
developers to serve templated web objects securely.

Among the planned applications:

* Serve capabilities to clients.  Serving capabilities from a template
  lets developers add them easily but allows administrators to control
  what is actually served.
* Serve prepopulated config files for popular setups.  For instance we
  can serve `.lakectl` and Hadoop/Spark config files.  These sometimes
  need to be modified, for instance when lakeFS does not know the name
  of its endpoint.

  This can be an important part of for time-to-value.  Using templates
  we offer admins some flexibility in configuration: for instance they
  might define templates with different values, and clients can select
  which to receive by path.
  
## Template format

We use Golang [text/template][text/template] to expand most templates.
For browsers (based on filename, e.g. `*.html.tt`) we instead must use
[html/template][html/template] for safety.

The template is read by lakeFS from a specific (configured) prefix, by
default `lakefs://lakefs-templates/main`.  IAM authorizes users to see
this template, at which point expansion can start.  We will supply the
following replacements (at least):

| template function | value source              | IAM permissions                              |
|:------------------|:--------------------------|:---------------------------------------------|
| config            | lakeFS configuration[^1]  | `arn:lakefs:template:::config/path/to/field` |
| object            | (small) lakeFS object     | IAM read permission for that object          |
| querystring       | URL query string[^2]      | (none)                                       |
| contenttype       | none; set `Content-Type:` | (none)                                       |

Adding a freeform dictionary to our config will allow admins to set up
any needed configuration.

We shall also supply the user object in `.user`.  We will probably add
new functions or objects.

## Examples

(All examples subject to change as we define object format, and decide
what functions are allowed.)

Set a template that looks like this:

```conf
spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem
spark.hadoop.fs.lakefs.access_key={{.user.credentials.access_key.id}}
spark.hadoop.fs.lakefs.secret_key={{.user.credentials.access_key.secret}}
spark.hadoop.fs.lakefs.endpoint={{"local.templates.lakefs_url" | config}}
# Or pass something from "local" to give users a different key.  In any case 
# requires IAM authorization to fetch.
spark.hadoop.fs.s3a.api.access_key={{"blockstore.s3.credentials.access_key_id" | config}}
spark.hadoop.fs.s3a.api.secret_key={{"blockstore.s3.credentials.secret_access_key" | config}}
```

This is a complete ready configuration for using lakeFSFS.  In future,
we could leverage it to provide more configuration, but it immediately
allows a single well-known point to set up.

A future lakeFSFS version might read from the configured endpoint when
it is loaded, allowing that to be the only required configuration.

[text/template]: https://pkg.go.dev/text/template
[html/template]: https://pkg.go.dev/html/template

[^1]: effectively includes environment variables!
[^2]: might instead be passed in as a template variable
