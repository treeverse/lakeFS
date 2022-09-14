# KV Store Querying Tool

## Scope

Our newly introduced KV Store stores the data in the form of [`Partition Key`, `Key`, `Value`] triplets. The `partition key` and `key` fields are strings stored as byte arrays and the `value` is an object (various types) serialized as protobuf and stored as byte array.
This data is not easily interpretable for human viewer, whereas our former SQL DB stored the data (mostly) as human readable data, making it easier for developers to look into the DB, and debug various situations. It also provided the same ease to insert data or alter existing data in the DB for experimentation and, in some cases, as a fix
With our new KV Store this became less trivial and this proposal comes to offer a solution for that

**Important Assumption:** This tool is meant to be an internal tool for us (`lakefs` developers) and as such it heavily relies on the KV internal knowledge we have (keys construction, hard-coded and generated partitions, etc)

## Introducing `lakefs kv`

The `lakefs` server contains all the logic needed to correctly read and update the KV Store. It generates objects into byte arrays with their protobuf encoding, on one hand, and rebuilds the objects from byte arrays with protobuf on the other, and, it holds all the logic to generate the relevant keys (e.g. `partition key` from a repository object, branch `key` from the branch object etc.)
We will elevate this logic and into a new command that will provide the ability to read data items, scan keys and update values, using a new `lakefs` command - `kv`
```
lakefs kv <cmd> <args>
```

## Implementation Steps

These steps describe the order and importance of the functionality we would like to get from `lakefs kv` and does not refer to specific commands format. The commands specified are for illustration purposes. The specifics of the commands format and their arguments are discussed in later section

### Step 1 - Get/Scan

* `lakefs kv get <args>`
* `lakefs kv scan <args>`

These 2 are `lakefs kv`'s MVP. They put the **query** in "**query**ing tool". Given some kind of identifier, these commands returns an object, or scans through a range of objects, outputting their details. The identifier can be a (`partition`,`key`) pair, or an object-type and its identifier (as used by lakefs, e.g. RepositoryID, BranchID etc.), depends on the approach we select - to be discussed later

### Step 2 - REPL

An interactive CLI prompt
```
$ lakefs kv repl

kv>
```
The possible commands are no other than the ones supported by `lakefs kv`, i.e. Get & Scan, and if step 3 is to be implemented, Set and Delete

The benefits of this approach is that unlike the `lakefs kv XXX` (where XXX is `get` or `set`), which runs as a dedicated process and terminates once input is generated, the `repl` command can execute multiple commands in the same process, saving the need to recreate data structures and connect to KV, as well as reducing the chance of failures due to errors in the above

### Step 3 - Set/Delete - If the need arises

* `lakefs kv set <args>`
* `lakefs kv delete <args>`

These commands can be added in case the need arises. The first is to alter/add a KV store entry, and the other is for removing an entry. These are very error prone operations that can easily result in data corruption, and so we will (a) not implement these right away, but only if they are really needed and (b) protect these with, at least, a requirement for command confirmation: `"Are you sure you want to delete entry? [yes/N]" etc.

## Different Approaches, Different Interfaces, Different Problems

### Full Knowledge

With this approach, `lakefs kv` is implemented with all the knowledge available. Knowledge like: how to generate a repository partition from the repository ID and object, how to generate a branch key from the repository and branch IDs, the fact that staged objects' keys cannot be provided as strings etc.
The commands in that case will be very specific, having the user specifying which entity he is working with. Essentially, this will be a CLI interface to various services, using KV (graveler, auth, etc.)
* `lakefs kv get-repository <REPO_ID>`
* `lakefs kv scan-staging <STAGING_TOKEN>` etc.
With this approach, `lakefs kv` will not have functionalities such as `get-staged-object` by key, or any other operation we know is not possible or does not make any sense

Pros:
* Implementation is pretty straight forward - we actually already have everything implemented, and only need to make it accessible via `lakefs kv` tool

Cons:
* Lack of flexibility - adding a new entity, requires to implement new commands (bring in the knowledge) in the `lakefs kv` tool
* Not really a "KV Querying" but more of a "services querying" tool. This might bring bugs from the existing logic to mask KV problems, or make them harder to identify

### Minimal Knowledge

With this approach, `lakefs kv` will have the minimal required knowledge of objects and their structures. It will present a generic KV-like interface that works with `partition`, `key` and `entries`.
* `lakefs kv get <partition> <key>`
* `lakefs kv scan <partition> [<key>]`
This implementation will acquire the required data dynamically at runtime, by having entities register their a key parsing/matching function, an object structure and any other needed data, I cannot think of at the moment. This approach relies on the fact that the `key` structure "hides" information about the entity type, and so `lakefs kv` can leverage that in a sort of reflection-like way, to access the correct entity and construct the corresponding object, in order to decode the entry data correctly and  display it

Pros:
* High level of flexibility - in order to introduce a new entity type, all one needs to do is to implement a correct object and a matcher/parser func, and register it. `lakefs kv` code has zero knowledge of the entities and all this knowledge is coming from the entities implementation
* `lakefs kv` interface remains unchanged - the commands are generic and are ready to handle newly introduced entities
* Accessing KV table the way it was meant to be accessed, which is quite what we are aiming for

Cons:
* This general approach might not hold, has not all entities can be treated the same - e.g. a repository object can be found, with his ID, under a hard-coded well known partition, but a branch need a repository in order to get its partition
  * Can be bypassed by counting on the user to provide the required partition correctly. i.e. in order to get a branch, one need to know the repository it belongs too (makes sense) get this repository using `lakefs kv`, generating the repository partition, and use the generated partition in to get the branch. This solution relies on the assumption that a user is an internal user (dev)
  * We can ease it up a bit by providing very specific ad-hoc commands such as `lakefs kv get-repo-partition`, to make the user life easier
* Non printable keys, such as staged object keys, may contain non printable chars. It will be impossible to use a `lakefs kv get` and specify a key for that. I think we can overcome this by blocking this specific functionality (get) and only allow `scan` for staging tokens. This will break the generality a bit, but I don't see any better option


