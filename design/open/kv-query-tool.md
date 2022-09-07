# KV Store Querying Tool

## Scope

Our newly introduced KV Store stores the data in the form of [`Partition Key`, `Key`, `Value`] triplets. The `partition key` and `key` fields are strings stored as byte arrays and the `value` is an object (various types) serialized as protobuf and stored as byte array.
This data is not easily interpretable for human viewer, whereas our former SQL DB stored the data (mostly) as human readable data, making it easier for developers to look into the DB, and debug various situations. It also provided the same ease to insert data or alter existing data in the DB for experimentation and, in some cases, as a fix
With our new KV Store this became less trivial and this proposal comes to offer a solution for that

## Introducing `lakefs kv`

The `lakefs` server contains all the logic needed to correctly read and update the KV Store. It generates objects into byte arrays with their protobuf encoding, on one hand, and rebuilds the objects from byte arrays with protobuf on the other, and, it holds all the logic to generate the relevant keys (e.g. `partition key` from a repository object, branch `key` from the branch object etc.)
We will elevate this logic and into a new command that will provide the ability to read data items, scan keys and update values, using a new `lakefs` command - `kv`
```
lakefs kv <cmd> <args>
```

### Get/Scan/Set

* `lakefs kv get <partition> <key>` </br>
  Given a `partition key` and a `key` (both as strings), returns the corresponding object (json format)

* `lakefs kv scan <partition> [<key>]` </br>
  Given a `partition key` and an optional `key`, returns all object in that partition that keys are equal or greater to the specified `key` (no `key` will return all objects in the partition). Results will be formatted as a json array

* `lakefs kv set <partition> <key> <value>` </br>
  Given a `partition key`, a `key` and a json formatted object, sets the value for the given `key` in the given `partition`, as an encoded protobuf for the given object

### Delete (?)
* `lakefs kv delete <partition> <key>` </br>
  Given a `partition key` and a `key`, deletes the corresponding entry from the KV Store
  **Question:** Do we really want this?

### The `<key>` is the Key
Our KV implementation creates different formatted `path`s to different object types, having a hint for the type encoded within the `path`. The specified `key` (for each operation) should match this `path` format, and as such, it hints for the object type.
The querying tool will use a `key-format` to `object` translation functionality where the given key will be matched
This will be elevated to use the correct object to decode either the read protobug (for Get/Scan), or given json (for set), to an actual object, and decode it to json/protobuf respectively
Upon process start, all `path` formatters will register with their corresponding object type, creating a format->object mapping, to be used.
This will make it easy to add new object types in the future, and register them as well.
**TBD:** How to implement and enforce this requirement?

## Open Questions
* How can we implement the registration of the `path` function to a `path` -> object translator? I think, that as a first step, going over all the currently known object types and registering them, will be sufficient
* How about partitions other than the trivial "auth" and "graveler"? Is it enough to leave it to the user to query for a repository, and calculate its `partition key`? Or should we add some ad-hoc commands for that (e.g. `lakefs kv get-repo-key <repo-id>`)?


