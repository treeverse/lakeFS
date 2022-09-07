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
For browsers (based on filename, e.g. `*.html`) we instead must use
[html/template][html/template] for safety.

Accesses to http://<lakefs>/api/v1/templates/some/path read the template
from `some/path`.

The template is read by lakeFS from a specific (configured) prefix, by
default `lakefs://lakefs-templates/`.  We might decide to support
various object storage types; `lakefs` gives the best management and lineage
capabilities, but possible supporting the production blockstore types `s3`,
`azure` and `gcs` will simplify deployment of some lakeFS deployments.

If storing on lakeFS, IAM authorizes users to see this template by
`fs:ReadObject`, at which point expansion can start.  If we want to store
templates on the blockstore types, we will probably need to add a new IAM
action type for template expansion, or alternatively overload
`fs:ReadObject` to support off-lakeFS URIs.  We will supply the following
replacements (at least):

| template function | value source                        | IAM permissions                              |
|:------------------|:------------------------------------|:---------------------------------------------|
| config            | exportable lakeFS configuration[^1] | `arn:lakefs:template:::config/path/to/field` |
| object            | (small) object on lakeFS            | IAM read permission for that object          |
| contenttype       | none; set `Content-Type:`           | (none)                                       |
| new_credentials   | new credentials added to user       | `auth:CreateCredentials`                     |

A configuration variable will be "exportable" if its struct field is tagged
`` `export:yes` ``, or (if implementing that is too hard) if its type
implements

```go
interface Exportable {
	Export() string
}
```

IAM is still checked for exportable configuration variables (which will be
ON by default for users, but can be removed).

Adding a freeform dictionary to our config will allow admins to set up
any needed configuration.

We shall supply the user object in `.user` and the parsed query args in
`.query`[^2], to allow conditional operations.  We will probably add new
functions or objects.

## Template expansion flow

#### _:warning: This flow assumes templates stored on lakeFS. :warning:_

1. User accesses the template
   http://<lakefs>/api/v1/templates/main/expand/me.
1. lakeFS expands to a lakeFS path (by default this is
   lakefs://lakefs-templates/main/expand/me).
1. lakeFS uses IAM to verify user has `fs:ReadObject` permission on this
   object.
1. Object is parsed into a text/template.
1. lakeFS creates template
   [`Funcs`](https://pkg.go.dev/text/template#FuncMap) that will check IAM
   permissions as required for expansion.
1. Template gets expanded and returned to user.

### Credentials

We supply a function `new_credentials` that creates a new set of credential
for the user, registers them, and returns them.  See this [example of template
functions returning multiple values][example_template_multiple].

This lets us implement the requirement to create and return new credentials
as part of a downloaded configuration.

## Examples

(All examples subject to change as we define object format, and decide
what functions are allowed.)

Set a template that looks like this:

```conf
spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem
{{with $creds := new_credentials}}
spark.hadoop.fs.lakefs.access_key={{$creds.ID}}
spark.hadoop.fs.lakefs.secret_key={{$creds.Secret}}
{{end}}
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
[example_template_multiple]: https://go.dev/play/p/1sfcodvxNvr

[^1]: note that this effectively includes environment variables!
[^2]: might instead be a function.
