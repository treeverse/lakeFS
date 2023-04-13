# Metadata for Airflow integration

## About

This is a design to integrate metadata from Airflow into lakeFS.  It
consists of metadata integration and UI for using it.

## Metadata integration

Metadata that Airflow operators and/or hooks may place on commits to
associate them with the generating Airflow.

### Stability

Commit _metadata_, like diamonds, are forever: It is not generally possible to
modify commit metadata.  Even if we were willing to rewrite commits and
could do so safely, it would still break commit digests as stable
identifiers.

### Forwards compatibility

Obviously we cannot guarantee any future metadata compatibility.  However
_where current implementation cost is unaffected_, this design is _forwards
compatible_:

* Airflow metadata added today should remain usable.
* **If** we ever add UI support for metadata from another (non-Airflow)
  system, it should work with metadata added today.

### Source of truth

A [wise woman once wrote](https://twitter.com/mipsytipsy/status/998084191488126976):

> You either have one source of truth, or multiple sources of lies.

There is a natural tension and thereby a continuoum between these two
extremes:

* **Normalized data.**  lakeFS commits hold only enough to identify an
  Airflow run - its URL.  Airflow holds all other run metadata, and is
  required to make use of the URL.
* **Denormalized data.**  lakeFS commits hold all the information that
  lakeFS and its users need about an Airflow run.  All run metadata is
  copied to lakeFS commit metadata.

We always need the normalized data pointer, it is the only real identifier
of the run.[^1] The lakeFS UI can offer the best integration when lakeFS is
the source of truth.  Initially we shall allow users to select what metadata
to copy.  The default will be _all_.  For velocity we will avoid adding any
metadata that is in any way difficult to produce in the lakeFS Airflow
commit operator.  We may revisit this decision after feedback from users.

[^1]: Naively the run ID would be sufficient.  But that imposes an
    assumption of a single Airflow instance.

### Scope

We add to each commit metadata for these items:

* DAG run.  The URL of the DAG run in Airflow that generated this commit.
  This is the URL of [Get a DAG run][get-dag-run] in the REST API.
* Optionally, additional DAG identifiers such as:
  * Git commit digest
  * Airflow system identifier
  * Airflow logical ("execution") date
  * Notes (an Airflow metadata concept)
  * Conf.

  We allow all items that can be retrieved by [Get a DAG run][get-dag-run]
  in the REST API, and copy terminology from there.

  All optionals must be exactly that.  Most importantly, conf can contain
  secrets and _must_ be optional.
  
### Naming

We define a simple naming scheme that allows some future-compatibility.  All
items use the prefix "`::lakefs::Airflow::`", staking a claim to the
"`::lakefs::`" prefix on commit metadata.

lakeFS supports only string metadata.  All plain suffixes are assumed to be
plain strings.  If a suffix ends in `[...]`, that encodes a _type_.  We
initially define the types that are needed to integrate Airflow.

We add these suffixes to encode how to access the DAG run:

* `url[url:id]`: The DAG run
* `url[url:ui]`: URL to access the Airflow UI for the run.

The REST API suggests the following suffixes:

* `dag_run_id`: string
* `dag_id`: string
* `logical_date[iso8601]`: ISO 8601

  `execution_date` is deprecated by Airflow and equal to `logical_date`, so
  it is _not_ included.
* `start_date[iso8601]`: ISO 8601
* `end_date[iso8601]`: ISO 8601
* `data_interval_start[iso8601]`: ISO 8601
* `data_interval_end[iso8601]`: ISO 8601
* `last_scheduling_decision[iso8601]`:  ISO 8601
* `run_type`: string
* `state`: string (presumably always "success")
* `external_trigger[boolean]`: boolean encoded as "`true`" or "`false`"
* `conf[json]`: string encoding a JSON object of conf
* `note`: string

#### Changing Airflows

If the domain name of the Airflow system changes then URLs stop working.
The DAG run URL is still a useful _identifier_, but it cannot be used to
query for information.

In a second phase, we may offer a simple future-proof method to allow
changing URLs: Configure a number of "URL endpoints" on lakeFS.  They will
look like variable expansions ("`http://$my_airflow/path/to/dag`") in commit
metadata.  Configuring the current Airflow base URL in the lakeFS
configuration allows both uniqueness and forwards compatibility to work.

Users are not required to use these shortcuts, of course -- and small or
test installations might choose to avoid them entirely.

## lakeFS UI

Integration of the Airflow UI into the lakeFS UI.  The UI will allow viewing
the associated metadata from the UI.  When viewing a commit, the Airflow UI
URL will become a link.  We shall also open the DAG in a frame above the
commit metadata if this is available.

### Flow

When displaying a commit, and if enabled, scan its metadata for URLs of type
`[url:ui]`.  If one is found, expand config variables in it and render it as a green button labelled "Open Airflow UI".

## Non-Airflow systems

Structured naming of metadata allows the UI to behave in a more generic
manner.  We can implement this shortly after implementing the original
Airflow-only UI.  It might optionally scan metadata for _all_ keys of the
form `::lakefs::Product::property"`, and use their types to display
correctly.

For instance, merely adding a property
```conf
::lakefs::GitHub::url[url:ui]=https://github.com/apache/airflow/tree/d16e54d16e54
```
should be enough to link a particular commit on GitHub, and even name it "GitHub".

## Future

### Framing Airflow

In future we might render the UI in an [HTML iframe][mdn-iframe]:

```html
<iframe title="Airflow UI" src="https://path/to/airflow"/>
```

Airflow can supposedly be framed in a UI.  By default this is allowed but it
can be [disabled by configuring][airflow-framing].

```ini
[webserver]
x_frame_enabled = False
```

This is phrased as a security advantage: it prevents clickjacking.
Unfortunately there is no way around this -- it's an HTML + website feature.

But it turns out that even this is hard:

* Airflow used to [treat the configuration `X_FRAME_ENABLED` _in
  reverse_][airflow-reversed-x-frame_enabled].  So even configuring it on an
  older Airflow version (2.2.4 and below) will be confusing, and many
  upgraded installations may have it backwards!
* The Astronomer login flow does not appear to understand this variable, and
  I was unable to embed Astronomer Airflow into an iframe.

[get-dag-run]:  https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_dag_run
[airflow-framing]:  https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/webserver.html#rendering-airflow-ui-in-a-web-frame-from-another-site
[mdn-iframe]:  https://developer.mozilla.org/en-US/docs/Web/HTML/Element/iframe
[airflow-reversed-x-frame-enabled]:  https://github.com/apache/airflow/blob/main/RELEASE_NOTES.rst#the-webserverx_frame_enabled-configuration-works-according-to-description-now-23222
