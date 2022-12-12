# 0-deployment hooks

## User Story

As a data engineer, I would like to perform metadata and schema validation automatically before committing and merging.
I would also like to easily integrate with external components like Athena and Hive Metastore.
I do not have the required permissions nor expertise to utilize Webhooks.

## Goals

The overarching goal is to substantially decrease the friction of using hooks, driving more lakeFS users to production use-cases.

1. Provide an easy, deployment-less way to use hooks - let users provide the logic they want with minimum configuration.
1. Support both `pre` and `post` hooks:
    1. `pre` hooks used to validate and test metadata and schema
    1. `post` hooks used to automatically expose data to downstream systems
1. Increase hooks discoverability by allowing 1-click UI driven setup for pre-configured hooks

## Non-Goals

1. Unlike other proposals in the past - avoid becoming a scheduler or deployment tool for Kubernetes/Docker/...

## High Level Design

Executing an external component (such as a container, sub-process, API or HTTP endpoint) has both operational implications as well as security implications.

What we really want is the ability to execute user-supplied logic when a certain event occurs in lakeFS. Databases have been doing so for decades now,
with things like Stored Procedures and Triggers: make the database itself the runtime instead of calling out to an external component.

This proposal suggests adding an embedded, programmable interface to lakeFS so that hooks could natively be run within lakeFS itself.

Embedding provides very tight control over the capabilities provided: expose a very tight API and programming interface: only a select set of functions and methods that can't "escape" the runtime environment.

A very common embedded language for such purposes is [Lua](https://www.lua.org/).

It is used as an embedded scripting language in many notable projects - From databases such as [Redis](https://redis.io/docs/manual/programmability/eval-intro/) and [Aerospike](https://docs.aerospike.com/server/architecture/udf) to load balancers such as [HAProxy](https://www.haproxy.com/blog/5-ways-to-extend-haproxy-with-lua/) and [NGINX](https://www.nginx.com/resources/wiki/modules/lua/) - all the way to games ([Roblox](https://create.roblox.com/docs/tutorials/scripting/basic-scripting/intro-to-scripting), [Gary's Mod](https://wiki.facepunch.com/gmod/Beginner_Tutorial_Intro) and others).

### Embedding Lua in lakeFS

Fortunately, most of the work of making Hooks pluggable has already been done. lakeFS currently supports WebHooks and Airflow `post-*` hooks so a general interface making this pluggable is already in place.

A feature-complete Lua VM in pure Go is available at [github.com/shopify/go-lua](https://github.com/Shopify/go-lua). It allows very fine-grained control on what user defined code is able to execute. It allows doing a few notable things that make it a good fit for hooks:

1. Provide control over available modules, including builtin ones - For example, we can simply not load the `io` or `os` modules at all, providing no access to the host environment.
1. Allow binding Go functions and structs to lua functions and tables - we can create our own set of exposed libraries that will be made available to user defined code.
1. An accompanying project [Shopify/goluago](https://github.com/Shopify/goluago) provides a great set of common libraries (`strings`, `regexp`, `encoding/json` and more) so that for the most part, we won't have to reinvent the wheel to provide something usable.
1. Allow injecting variables into the Lua VM's global table (`_G`) - this makes exposing metadata about the action easy and intuitive

### Defining Lua Hooks

Lua code could be supplied either in-line inside the `.yaml` hook definition file as follows:

```yaml
name: dump_all
on:
  post-commit:
  post-merge:
  pre-commit:
  pre-merge:
  pre-create-tag:
  post-create-tag:
  pre-create-branch:
  post-create-branch:
hooks:
  - id: dump_event
    type: lua
    properties:
      script: |
        json = require("encoding/json")
        print(json.marshal(action))
```

Here we're specifying 2 important properties:

- `hooks[].type = "lua"` - will cause the execution of the LuaHook type
- `hooks[].properties.script (string)` - an in-line lua script, directly in the yaml file.

Additionally, for more complex logic, it's probably a good idea to write the code as its own dedicated object, supplying a lakeFS path instead of an inline script.
Example:

```yaml
name: auto symlink
on:
  post-create-branch:
    branches:
      - view-*
  post-commit:
    branches:
      - view-*
hooks:
  - id: symlink_creator
    type: lua
    properties:
      args:
        # Export configuration
        aws_access_key_id: "{{ENV.AWS_ACCESS_KEY_ID}}"
        aws_secret_access_key: "{{ENV.AWS_SECRET_ACCESS_KEY}}"
        aws_region: us-east-1
        # Export location
        export_bucket: athena-views
        export_path: lakefs/exposed-tables/
        # Tables to export:
        sources:
          - tables/users/
          - tables/events/
      script_path: scripts/s3_hive_manifest_exporter.lua
```

This adds the following settings:

- `hooks[].properties.script_path (string)` - a path in the same lakeFS repo for a lua script to be executed
- `hooks[].properties.args (map[string]interface{})` - a map that will be passed down to the lua script as a global variable called `args`.


### Lua API exposed to hooks

The implementation will provide the following modules:

- `_G` - a modified set of builtin functions that a lua runtime provides. A few functions will be removed/modified:
    - `loadfile` - removed, since we don't want free access to the local filesystem
    - `dofile` - removed, since we don't want free access to the local filesystem
    - `print` - modified to not write to os.Stdout but instead accept an external `bytes.Buffer` so that output reaches the action log
- `aws/s3`:
    - `get_object`
    - `put_object`
    - `list_objects`
    - `delete_object`
    - `delete_recursive`
- `crypto/aes` (taken from *goluago*):
    - `encryptCBC`
    - `decryptCBC`
- `crypto/hmac` (taken from *goluago*):
    - `signsha256`
    - `signsha1`
- `crypto/sha256` (taken from *goluago*):
    - `digest`
- `encoding/base64` (taken from *goluago*):
    - `encode`
    - `decode`
    - `urlEncode`
    - `urlDecode`
- `encoding/hex` (taken from *goluago*):
    - `encode`
    - `decode`
- `encoding/json` (taken from *goluago*):
    - `marshal`
    - `unmarshal`
- `encoding/parquet`:
    - `get_schema`
- `regexp` (taken from *goluago*):
    - `match`
    - `quotemeta`
    - `compile`
- `path`:
    - `parse`
    - `join`
    - `is_hidden`
- `strings` (taken from *goluago*):
    - `split`
    - `trim`
    - `replace`
    - `has_prefix`
    - `has_suffix`
    - `contains`
- `time` (taken from *goluago*):
    - `now`
    - `format`
    - `formatISO`
    - `sleep`
    - `since`
    - `add`
    - `parse`
    - `parseISO`
- `uuid` (taken from *goluago*):
    - `new`

Additionally, a lakeFS client pre-authenticated with the user that performed the action will be loaded:
This client doesn't go over the network - it generates in-process `http.Request` objects that are then passed directly into the current process' `http.Server`.

- `lakefs`
    - `create_tag`
    - `diff_refs`
    - `list_objects`
    - `get_object`

Other methods or calls could be added over time.

## Hooks Discoverability

This is nice to have and not necessarily part of a first release, but once we have embedded hooks, we can do the following:

1. add a `scripts/` directory to new repositories that contains reusable lua scripts for common use cases
1. suggest auto-creating a hook at certain touch points:
    - on new commit modal: when adding a k/v pair, suggest adding a hook to validate the existence of this key in subsequent commits
    - when committing a change/viewing an object tree that includes parquet/orc files - automatically add hook that validates breaking schema changes
    - when looking at a hive-partitioned table - automatically add a hook to export symlinks for Athena/Trino/...
    - when creating a branch, automatically add a hook to register tables on this branch in a metastore
    - when doing any versioning action - add simple hook to print out information about commits/merges/branches/tags when they happen
    - when tagging, add a hook to enforce a naming convention to tags
    - when merging, add a hook to ensure formats used, schema blacklist, etc.
1. On the "Actions" tab, do a marketplace-like view of available hooks that could be added with a click (essentially the list above but centralized)

## Limitations/Downsides

1. While Lua is popular as an embedded language for server components and databases, it is not a common language overall. It is very simple but we can assume the vast majority of users will not have any prior experience with it.
1. While this solves some of the friction of having to run something external, it doesn't solve another underlying problem with hooks: long running `pre-` actions are still tied to the lifecycle of the HTTP requests. This has to be solved by another mechanism
1. Users still need to configure the YAML files that tie together the lua logic with the events in the system. The usability and discoverability of that are still open questions.

## Alternatives

Another option is to introduce WASM support. This theoretically allows writing hooks in many languages while getting those same guarantees.
This, however, has several limitations:

1. Users will either have to compile their code to wasm themselves, or lakeFS would have to compile code for them on the fly
2. The [WASM Specification](https://webassembly.github.io/spec/core/) has a much larger surface area: supporting it while also making sure user functions cannot "escape" the hook runtime would be harder, making this less secure
3. Support for dynamic languages in WASM is still in early stages. Most importantly, Python, which is the most common language for data engineering and ML has only limited support. The most notable wasm compiler being [pyodide](https://github.com/pyodide/pyodide) which currently can only expose a REPL over a web interface and doesn't allow headless compilation to wasm.

Supporting Lua doesn't close the door on supporting WASM in the future as well and is much faster and simpler to implement.