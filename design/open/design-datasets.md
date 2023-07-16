## Existing

We have these forms of table definitions.  I order them here roughly by
chronological order of initial implementation, mixed with mutual
dependencies.

### `lakectl metastore`

This command supports *Hive* and *Glue* metastores.

A Hive metastore is stored off lakeFS on a Hive Metastore Server (HMS).
`lakectl` allows updating the table stored on the HMS to match branch
states, and can drive HMS merges and copies.  All metadata is stored on the
HMS; the `lakectl metastore` command calls out to a HMS URI that is supplied
by configuration or by flags.

A Glue or similar metastore is stored off lakeFS.  lakeFS exports "symlinks"
objects that give it direct access to S3.  All important table metadata is
still stored on an external Glue catalog.

### Delta Lake and Delta-diff

Given a path, Delta describes its own schema, so no further catalog is
necessary.  The current lakeFS Delta Lake implementation is unaware of the
list of tables stored in the repository.

Delta diff has no explicit concept of tables because it needs none.  The web
UI activates Delta diff (only) when a folder contains a `_delta_log/`
subfolder.

It will be possible to list all subfolders holding Delta Lake folders by
listing with separator `_delta_log/`.  (A simple API change could add a
"prefixes-only" mode to the lakeFS list-objects API, if needed.)

### Iceberg Catalog

lakeFS Iceberg support uses a catalog to locate Iceberg files.  This allows
it to translate schema to refs.  But the lakeFS Iceberg catalog wraps a
Spark catalog; it does not store any tables itself.  Currently only
SparkCatalog is supported, but it is not clear what _other_ catalogs would
work.

### Delta Sharing server

The Delta Sharing server is available on lakeFS Cloud.  It is used to
connect lakeFS to DataBricks Unity Catalog, along with similar support in
other products.  The Delta Sharing protocol requires each Delta Sharing
server to be a catalog for the tables that it manages.  So lakeFS Delta
Sharing defines tables.

The lakeFS Delta Sharing server defines tables by using YaML files under the
`_lakefs_tables/` prefix.  Each table is defined by its own YaML file.  The
server translates the schema of a table identifier to a ref.  By specifying
paths through object keys these definitions exactly support lakeFS ref
semantics: branching out, tagging, ref expressions, and even successfully
merging defines the same table across schemas.

lakeFS Delta Sharing supports two table types:

* **Delta Lake**: These describe their own schema.  A `delta` table needs no
  information beyond a table name and its path.
* **"Hive"**: These are partitioned directories of Parquet or possibly other
  files.  Despite the name, no Hive metastore is involved; rather the lakeFS
  Delta Sharing server defines similar semantics.  A `hive` table needs a
  name and a path, but also a list of partition columns and a schema for all
  fields.  Table schemas are defined using
  [this][delta-sharing-schema-schema] "a subset of Spark SQL's JSON Schema
  representation".  Currently Hive-style tables do not support column ranges
  metadata.

## Analysis

We currently support these 3 ways of defining tables:

* Implicitly, by discovering that a given path holds a table.  This is our
  current Delta Lake and Delta-diff support.
* Externally, by deferring to an external catalog.  This is our metastore
  support (HMS is the catalog) and our Iceberg support (SparkCatalog is the
  catalog).
* Internally, by effectively serving the catalog.  This is our lakeFS Delta
  Sharing, except that the Delta Sharing protocol does not call this a
  "catalog".

### Implicit definitions

Implicit definitions give users the _easiest_ path.  However they have
multiple limitations:

* Require discoverability of the table by examining only object metadata.
  So supported only by "modern" table formats: Iceberg and Delta.  Formats
  such as prefixes containing Parquet files _cannot_ support all operations:
  - It is not possible to report table metadata efficiently.
  - It may not be possible to list all tables.
  - It will _not_ be possible to support named tables, all tables must be
    defined by paths.

Using implicit definitions necessarily leaks through the table abstraction.
There is no useful general case here.  Users will find it easy to request
functionality that uses implicit definitions.

#### Possible future features

Define an implicit "Parquet folder" as a folder that holds many `.parquet`
files, or a folder that holds many `<key>=<value>` subfolders with some
`.parquet` files under them.  Then we could add to the GUI a display of a
sample DuckDB query when looking at a Parquet folder:

### External definitions

External definitions in catalogs may be the easiest way to give _some_ users
a fully-functional experience, for _some_ catalogs.  Their principal
limitations:

* Risk of fragmentation: If there are many popular external catalogs we will
  need to support many of them.  Different catalogs support different
  use-cases, so we should expect to have to support multiple catalogs.
* Limitations due to the external definition: Because existing catalog
  implementations were designed without lakeFS in mind, we will not be able
  to add lakeFS features to all of them.  For instance the HMS protocol is
  Thrift-based and does not support refs-as-schemas.

#### Possible future features

Support more catalog types.  Each catalog adds implementation effort.

Support catalog wrappers that work with multiple external catalogs.  For
instance, support wrapping additional catalogs under the Iceberg catalog.

### Internal definition

An internal definition should allow us to support _all_ lakeFS features that
systems can handle.  Because it reduces external dependencies, it is
probably the fastest way for us to implement "table" features on top of
lakeFS -- we have seen this in the Iceberg catalog wrapper which is barely
more than an internal definition, and in the lakeFS Delta Sharing server.
Their principal limitations:

* Possibly unacceptable cost to users: If users are deeply invested in
  existing catalogs they may be reluctant or unable to transition to lakeFS
  catalogs.  Again, wrappers might mitigate this.
* Risk of fragmentation: There may be different base catalog interfaces for
  different use-cases.  For instance, the Iceberg catalog implementation is
  a particular _kind_ of Spark catalog, and will not be suitable for
  straight Delta Lake tables.  This fragmentation is _considerably less_
  than the fragmentation expected for external definitions, of course.
* Becoming opinionated.  lakeFS is so far unopinionated as regards catalogs:
  it defines none and has difficulty integrating with most.  As soon as we
  define catalogs we run the risk of becoming opionated in ways that our
  users might prefer to avoid.

  One way to decrease this is to implement lakeFS table definitions as a
  separate lakeFS layer, and catalogs as yet other layers.  Users must still
  be able to use lakeFS without these extras.

#### Possible future features

Support catalogs for multiple implementations: general Spark and Hadoop,
non-Java ecosystem, etc.

Support translation from existing (external) catalogs.

Support repository / ref scanning to define useful draft table definitions.

## Questions for our design partners

The key questions to answer are:

* _How many tables have users already defined_?  (Affects whether we need to
  support external tables).

  Follow-on:
  - _Which existing catalogs do users have?_
  - _Can we automatically derive _internal_ definitions from _external_
    definitions for these catalogs?_

* _How often do users define new tables?_  (Affects going to implicit or
  explicit internal/external definitions)

* _How much user effort is required to define a new table?_  (Affects going to implicit or
  explicit internal/external definitions)

  Follow-on:
  - _How far can we automate this process?_  (Affects going to external or
    internal explicit definitions)
  - _Do users already have tools that automate this process?_

* _How much do users need external catalogs?_  (Affects whether we need to
  support external tables)
* _How many external catalog types do our users use?_  (Affects whether we need to
  support external tables; many different types will dilute our effort).

## Preferred route

This route requires support from design partners in the "Questions for our
design partners".  I propose it as a sample guide for how we might proceed
if all questions resolve in a certain way.

* Define the current lakeFS Delta-Sharing tables schema as the lakeFS tables
  schema.
* Possible extensions:
  - If there is demand for efficient `hive` format, add support for storing
    index metadata.
* Alternatives for Spark catalog support:
  - Write a Spark catalog implementation.
  - Write translators from lakeFS tables to some popular external catalogs,
    for users who need to continue to use those.
* Write or adapt existing helper tools to help draft lakeFS table schema
  from object listings.


[delta-sharing-schema-schema]:  https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#schema-object
