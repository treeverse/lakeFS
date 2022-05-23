# Implementing Auth Package with KV Database

This document describes the entities of lakeFS auth package, the relationship between them and offers
a possible representation of them in the KV database.

## Entities

Some entities, like `User` and `Group` has an `ID` field which is a serial int key handled by postgres. 
It will be migrated to a generated random string ID.

### User

- Can be looked up by `ID`, `Username` & `Email`.
- Deleted by `Username`.
- Listed by `Username`.

### Group

- Looked up, deleted and listed by `DisplayName`.

### Policies

- Looked up, deleted and listed by `DisplayName`.

### Credentials

- Has a foreign id reference to `User.ID`.
    - The `User.ID` is used for looking up the `User.Username`.
    - Almost all actions are in the context of `User.Username`, i.e. it is always passed.
- The only action without a `User.Username` passed is credentials lookup by `AccessKeyID`.


## Entities Relationship

### Group <-> User

- Add/Remove `User` to `Group` by `User.Username` & `Group.DisplayName`. 
- List all the `Group`s for a `User` by `User.Username`.
- List all the `Users`s in a `Group` by `Group.DisplayName`.

### Policy <-> User

- Attach/Detach `Policy` to `User` by `User.Username` & `Policy.DisplayName`.
- List all `Policies` for a `User` by `User.Username`.
- List all effective `Policies` for a `User` by `User.Username`, this includes all `Group Policies` for Groups the user is a member of.

### Policy <-> Group

- Attach/Detach `Policy` to `Group` by `Group.DisplayName` & `Policy.DisplayName`.
- List all `Policies` for a `Group` by `Group.DisplayName`.
    - `Group` cannot be a member of another `Group`, therefore all effective policies are attached to it directly.

## Representation in the KV world

- All keys are prefixed by the reserved package prefix `auth/`.
  
- `User`s key will be in the form of `users/<UserName>`.
- `Group`s key will be in the form of `groups/<DisplayName>`.
- `Policies`s key will be in the form of `policies/<DisplayName>`.
- `Credentials`s key will be in the form of `users/<UserName>/credentials/<AccessKeyID>`.

### Handling Secondary Indexes

Below are 2 options that vary in efficiency and complexity of the implementation.
 
#### Store just the minimum indexes

Keep only the minimum that is required to represent the relationship of the entities:

- A `User` membership of a `Group` under `groups/<DisplayName>/users/<Username>`.
- A `Policy` attached to a `User` under `users/<Username>/policies/<DisplayName>`.
- A `Policy` attached to a `Group` under `groups/<Displayname>/policies/<DisplayName>`.

Any deletion of an entity would first remove all its secondary indexes, then the entity itself.
For example, deleting a `Policy` would have to list all `User`s & `Group`'s `Policies` and delete any attachment if existed.
Only then it should delete the `Policy` entity from the store.

Listing by anything other than the key will require to list all the entities which are relevant.
Some examples:

1. Finding a `User` by `AccessKeyID` will require to list all the users.
1. Listing the `User`'s effective policies will require to list all entities under `groups/`.

The amount of entities in the Auth world isn't big (<10k) and cached in the server, it's unlikely it will incur a 
notable performance degradation.

#### Store all secondary indexes

In addition to the indexes in the above suggestion, also store:

- A `User` membership of a `Group` under `users/<Username>/groups/<DisplayName>`.
- `Credentials` attached to a `User` under `credentials/<AccessKeyID>/<Username>`.
- A `Policy` attached to a `User` under `policies/<DisplayName>/users/<Username>`.
- A `Policy` attached to a `Group` under `policies/<DisplayName>/groups/<DisplayName>`.

Every operation that updates a relationship between two entities would have to be stored everywhere.
The upside is a possible increase in performance ("possible" since we read less entities, but perform much more round-trips to the cache/store).

### Working with no locks

In the Postgres era, we rely on it to keep the entities and relationship consistent.
For example, a deleted `Policy` would cascade to the `auth_user_policies` & `auth_group_policies` tables.


In the KV world, we must cleanup the secondary indexes first, before deleting the entity itself.
Since the KV is lock-free, we might still get into troubles.
For example, a `Policy` is requested to be deleted. The operation starts by listing all `User`s & `Group`s with that `Policy` attached to it.
While iterating, it's possible that another unlisted `User` would be attached to the `Policy`. Once the `Policy` is deleted,
we would remain with an attachment to it. There are at least 2 mitigations for it:

1. Do (a) list and delete of secondary indexes (b) delete the entity (c) another round of list and delete.
2. Any store access should consider that the secondary indexes are fragile, and should always retrieve the entities pointed by it.
The "truth" is in the entities themselves and not in the secondary index.

### Decision

Due to its simplicity and without clear evidence of lakeFS installations with many users, I believe that option #1 is better.
As OIDC would likely be introduced soon, it's even likelier that users will handle the auth part elsewhere for installations with many users.
