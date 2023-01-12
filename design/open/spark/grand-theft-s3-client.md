# Getting S3 Clients in Spark Clients

## Why?

We have 2 clients for Spark that work on lakeFS but also need to access AWS
S3 directly:

<table>
  <tr>
	<th>Client</th><th>Where used</th><th>Why it needs an S3 client</th>
  </tr><tr>
	<td>Spark Metadata client</td>
	<td>
	  GC (both committed and uncommititted), Spark Export, and also
      available for users to use to access lakeFS metadata directly.
	</td><td>
	  Accesses stored metadata directly on S3 and deletes data objects.
	</td>
  </tr><tr>
	<td>
	  lakeFSFS
	</td>
	<td>
	  Reading and writing directly on lakeFS.
	</td>
	<td>
	  Read ETags of uploaded objects to put them in lakeFS metadata.  In
      _some_ Hadoop versions, the S3AFileSystem returns FileStatus objects
      with a S3AFileStatus.getETag method.  Otherwise a separate call to S3
      is needed.
	</td>
  </tr>
</table>

![David Niven as Sir Charles Litton, The Pink Panther][pink-panther-img]

These Spark clients cannot work without a working S3 client[^1].  This is:

* **Different** between our two clients.

  The Spark metadata client supports _only_ authentication to S3 using
  access keys or STS, lakeFSFS supports _only_ taking ("_stealing_") clients
  from S3AFileSystem.

* **Brittle**.

  Some users cannot use the authentication methods that we make available to
  them.  The thievery code in lakeFSFS is subtle and greatly depends on an
  assumed underlying implementation; it can break when DataBricks introduce
  new features.
* **Uninformative** in the case of system or user error.

  Users receive very poor error reports.  If they get as far as an S3 client
  but it is misconfigured, S3 happily generates "400 Bad Request" messages.
  If client theft fails, it generates a report of the _last_ failure --
  probably not the most _important_ failure.

There are numerous bug reports and user questions about this area.

## What?

We propose to:

1. **Reduce friction.**  When S3A already works on a Spark installation,
   users should typically not have to add _any_ S3-related configuration in
   order to use lakeFS clients.
1. **Unify** S3 client acquisition between the two clients.  Both clients will
   support the same configuration options: clients "stolen" from the
   underlying S3AFileSystem, and explicitly created clients with static
   access keys and STS.  Prefer stealing clients to creating them -- these
   are most likely to work.
1. **Improve** error reporting.  Report the stages attempted and how each
   one failed.
1. **Create a more general scheme** for generating clients.  Over time we
   can hope to support more underlying implementations.

## Design principles

Unify client generation code into a single library.  We will be able to test
this library individually on various Spark setups.  This will probably not
be automatic -- there is no automatic source for _new_ Spark setups, and it
is not clear how often _existing_ Spark setups change.  But even being able
to run a single command on a Spark cluster and get useful information will
be very useful for investigation, helping customers probe their setup, and
further development to support setups where we fail.

This library will define an interface for _client acquisition_: given
various parameters TBD (perhaps a SparkContext or a Hadoop configuration), a
path, and optionally also a FileSystem on that path, a client acquisition
attempt returns a client or a failure message.

A future version may well generalize to acquiring a client for other
underlying storage types from other FileSystems.

The library will include code that tries each of a list of strategies, in
order of desirability.  It will return a client or throw some exception with
a detailed method.  And it will report which strategy was actually used to
acquire the client.  To increase performance, the library will cache to
client used by FileSystem.  This will typically mean that the acquisition
code is called just once.

The list of strategies will be configurable on a Hadoop property.
Additionally we will create pre-populated lists, one recommended for
no-hassle production use and the other consisting of all (or almost)
strategies that will be recommended for debugging.  Users who explicitly
wish to use a single strategy will simply configure that one strategy as the
only option.

One complication is that many FileSystems are _layered_ and strategies to
detect them may require some recursion or at least iteration.  For instance,
while S3A may support `S3AFileSystem.getAmazonS3Client`, on DataBricks we
might have to unwrap it from `CredentialScopeFileSystem` using
`CredentialScopeFileSystem.getReadDelegate`, and then try to acquire an S3
client from whatever is returned.

The type of the returned client is indeterminate to the caller.  It _is_ an
AmazonS3Client with desired authentication and the ability to connect to the
bucket.  But it may well be one of a different version or package than the
caller expects, and if the caller so much as attempts to cast it to
AmazonS3Client it will get a nasty ClassCastException.  Similarly for its
*Request and *Response objects.  Everything should be done using reflection,
and the library should also help with this call.[^2]

[^2]: The current code assumes that the expected Request object has the same
    actual type and is compatible.  This is a bug and will surely break
    somewhere.

### Example: information a strategy might return

One method of generating a FileSystem is to call `getWrappedFs` _if_ that
FileSystem has such a call, and recurse on that.  When such a strategy fails,
it should report:

1. The dynamic type of FileSystem that it received.
1. What failed:
   1. `getWrappedFs`?  For instance, if it received a FileSystem that does
      not have this method.
   1. Acquiring a FileSystem from the wrapped instance?  This is a recursive
      attempt, and its failures will also include information about failure.


[pink-panther-img]:  https://static.wikia.nocookie.net/pinkpanther/images/7/76/David_Niven_-_01.webp/revision/latest?cb=20220531105637

[^1]: lakeFSFS _might_ not need the client, if it can find an ETag on
    returned FileStatus objects.
