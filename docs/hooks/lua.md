---
layout: default
title: Lua Hooks
parent: Actions and Hooks
grand_parent: Reference
description: Lua Hooks reference 
---


# Lua Hooks

lakeFS supports running hooks without relying on external components using an [embedded Lua VM](https://github.com/Shopify/go-lua)

Using Lua hooks, it is possible to pass a Lua script to be executed directly by the lakeFS server when an action occurs.

The Lua runtime embedded in lakeFS is limited for security reasons. It provides a narrow set of APIs and functions that by default do not allow:

1. Accessing any of the running lakeFS server's environment
2. Accessing the local filesystem available the lakeFS process

{% include toc.html %}

## Example Lua Hooks

### Display information about an event

This example will print out a JSON representation of the event that occurred:

```yaml
name: dump_all
on:
  post-commit:
  post-merge:
  post-create-tag:
  post-create-branch:
hooks:
  - id: dump_event
    type: lua
    properties:
      script: |
        json = require("encoding/json")
        print(json.marshal(action))
```

### Ensure that a commit includes a mandatory metadata field

A more useful example: ensure every commit contains a required metadata field:

```yaml
name: pre commit metadata field check
on:
pre-commit:
    branches:
    - main
    - dev
hooks:
  - id: ensure_commit_metadata
    type: lua
    properties:
      args:
        notebook_url: {"pattern": "my-jupyter.example.com/.*"}
        spark_version:  {}
      script: |
        regexp = require("regexp")
        for k, props in pairs(args) do
          current_value = action.commit.metadata[k]
          if current_value == nil then
            error("missing mandatory metadata field: " .. k)
          end
          if props.pattern and not regexp.match(props.pattern, current_value) then
            error("current value for commit metadata field " .. k .. " does not match pattern: " .. props.pattern .. " - got: " .. current_value)
          end
        end
```

For more examples and configuration samples, check out the [examples/hooks/](https://github.com/treeverse/lakeFS/tree/master/examples/hooks) directory in the lakeFS repository.

## Lua Library reference

The Lua runtime embedded in lakeFS is limited for security reasons. The provided APIs are shown below.

### `aws/s3.get_object(bucket, key)`

Returns the body (as a Lua string) of the requested object and a boolean value that is true if the requested object exists

### `aws/s3.put_object(bucket, key, value)`

Sets the object at the given bucket and key to the value of the supplied value string

### `aws/s3.delete_object(bucket [, key])`

Deletes the object at the given key

### `aws/s3.list_objects(bucket [, prefix, continuation_token, delimiter])`

Returns a table of results containing the following structure:

* `is_truncated`: (boolean) whether there are more results to paginate through using the continuation token
* `next_continuation_token`: (string) to pass in the next request to get the next page of results
* `results` (table of tables) information about the objects (and prefixes if a delimiter is used)

a result could in one of the following structures

```lua
{
   ["key"] = "a/common/prefix/",
   ["type"] = "prefix"
}
```

or:

```lua
{
   ["key"] = "path/to/object",
   ["type"] = "object",
   ["etag"] = "etagString",
   ["size"] = 1024,
   ["last_modified"] = "2023-12-31T23:10:00Z"
}
```

### `aws/s3.delete_recursive(bucket, prefix)`

Deletes all objects under the given prefix

### `crypto/aes/encryptCBC(key, plaintext)`

Returns a ciphertext for the aes encrypted text

### `crypto/aes/decryptCBC(key, ciphertext)`

Returns the decrypted (plaintext) string for the encrypted ciphertext

### `crypto/hmac/sign_sha256(message, key)`

Returns a SHA256 hmac signature for the given message with the supplied key (using the SHA256 hashing algorithm)

### `crypto/hmac/sign_sha1(message, key)`

Returns a SHA1 hmac signature for the given message with the supplied key (using the SHA1 hashing algorithm)

### `crypto/md5/digest(data)`

Returns the MD5 digest (string) of the given data

### `crypto/sha256/digest(data)`

Returns the SHA256 digest (string) of the given data

### `encoding/base64/encode(data)`

Encodes the given data to a base64 string

### `encoding/base64/decode(data)`

Decodes the given base64 encoded data and return it as a string

### `encoding/base64/url_encode(data)`

Encodes the given data to an unpadded alternate base64 encoding defined in RFC 4648.

### `encoding/base64/url_decode(data)`

Decodes the given unpadded alternate base64 encoding defined in RFC 4648 and return it as a string

### `encoding/hex/encode(value)`

Encode the given value string to hexadecimal values (string)

### `encoding/hex/decode(value)`

Decode the given hexadecimal string back to the string it represents (UTF-8)

### `encoding/json/marshal(table)`

Encodes the given table into a JSON string

### `encoding/json/unmarshal(string)`

Decodes the given string into the equivalent Lua structure

### `encoding/parquet/get_schema(payload)`

Read the payload (string) as the contents of a Parquet file and return its schema in the following table structure:

```lua
{
  { ["name"] = "column_a", ["type"] = "INT32" },
  { ["name"] = "column_b", ["type"] = "BYTE_ARRAY" }
}
```

### `lakefs`

The Lua Hook library allows calling back to the lakeFS API using the identity of the user that triggered the action.
For example, if user A tries to commit and triggers a `pre-commit` hook - any call made inside that hook to the lakeFS
API, will automatically use user A's identity for authorization and auditing purposes.

### `lakefs/create_tag(repository_id, reference_id, tag_id)`

Create a new tag for the given reference

### `lakefs/diff_refs(repository_id, lef_reference_id, right_reference_id [, after, prefix, delimiter, amount])`

Returns an object-wise diff between `left_reference_id` and `right_reference_id`.

### `lakefs/list_objects(repository_id, reference_id [, after, prefix, delimiter, amount])`

List objects in the specified repository and reference (branch, tag, commit ID, etc.).
If delimiter is empty, will default to a recursive listing. Otherwise, common prefixes up to `delimiter` will be shown as a single entry.

### `lakefs/get_object(repository_id, reference_id, path)`

Returns 2 values:

1. The HTTP status code returned by the lakeFS API
1. The content of the specified object as a lua string

### `path/parse(path_string)`

Returns a table for the given path string with the following structure:

```lua
> require("path")
> path.parse("a/b/c.csv")
{
    ["parent"] = "a/b/"
    ["base_name"] = "c.csv"
} 
```

### `path/join(*path_parts)`

Receives a variable number of strings and returns a joined string that represents a path:

```lua
> require("path")
> path.join("path/", "to", "a", "file.data")
path/o/a/file.data
```

### `path/is_hidden(path_string [, seperator, prefix])`

returns a boolean - `true` if the given path string is hidden (meaning it starts with `prefix`) - or if any of its parents start with `prefix`.

```lua
> require("path")
> path.is_hidden("a/b/c") -- false
> path.is_hidden("a/b/_c") -- true
> path.is_hidden("a/_b/c") -- true
> path.is_hidden("a/b/_c/") -- true
```
### `path/default_separator()`

Returns a constant string (`/`)

### `regexp/match(pattern, s)`

Returns true if the string `s` matches `pattern`.
This is a thin wrapper over Go's [regexp.MatchString](https://pkg.go.dev/regexp#MatchString){: target="_blank" }.

### `regexp/quote_meta(s)`

Escapes any meta-characters in string `s` and returns a new string

### `regexp/compile(pattern)`

Returns a regexp match object for the given pattern

### `regexp/compiled_pattern.find_all(s, n)`

Returns a table list of all matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned)

### `regexp/compiled_pattern.find_all_submatch(s, n)`

Returns a table list of all sub-matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned).
Submatches are matches of parenthesized subexpressions (also known as capturing groups) within the regular expression,
numbered from left to right in order of opening parenthesis.
Submatch 0 is the match of the entire expression, submatch 1 is the match of the first parenthesized subexpression, and so on

### `regexp/compiled_pattern.find(s)`

Returns a string representing the left-most match for the given pattern in string `s`

### `regexp/compiled_pattern.find_submatch(s)`

find_submatch returns a table of strings holding the text of the leftmost match of the regular expression in `s` and the matches, if any, of its submatches

### `strings/split(s, sep)`

returns a table of strings, the result of splitting `s` with `sep`.

### `strings/trim(s)`

Returns a string with all leading and trailing white space removed, as defined by Unicode

### `strings/replace(s, old, new, n)`

Returns a copy of the string s with the first n non-overlapping instances of `old` replaced by `new`.
If `old` is empty, it matches at the beginning of the string and after each UTF-8 sequence, yielding up to k+1 replacements for a k-rune string.

If n < 0, there is no limit on the number of replacements

### `strings/has_prefix(s, prefix)`

Returns `true` if `s` begins with `prefix`

### `strings/has_suffix(s, suffix)`

Returns `true` if `s` ends with `suffix`

### `strings/contains(s, substr)`

Returns `true` if `substr` is contained anywhere in `s`

### `time/now()`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).

### `time/format(epoch_nano, layout, zone)`

Returns a string representation of the given epoch_nano timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }.

### `time/format_iso(epoch_nano, zone)`

Returns a string representation of the given `epoch_nano` timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The returned string will be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format.

### `time/sleep(duration_ns)`

Sleep for `duration_ns` nanoseconds

### `time/since(epoch_nano)`

Returns the amount of nanoseconds elapsed since `epoch_nano`

### `time/add(epoch_time, duration_table)`

Returns a new timestamp (in nanoseconds passed since 01/01/1970 00:00:00) for the given `duration`.
The `duration` should be a table with the following structure:

```lua
> require("time")
> time.add(time.now(), {
    ["hour"] = 1,
    ["minute"] = 20,
    ["second"] = 50
})
```
You may omit any of the fields from the table, resulting in a default value of `0` for omitted fields

### `time/parse(layout, value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).
This timestamp will represent date `value` parsed using the `layout` format.

The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }

### `time/parse_iso(value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00 for `value`.
The `value` string should be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format

### `uuid/new()`

Returns a new 128-bit [RFC 4122 UUID](https://www.rfc-editor.org/rfc/rfc4122){: target="_blank" } in string representation.

### `net/http` (optional)

Provides a `request` function that performs an HTTP request.
For security reasons, this package is not available by default as it enables http requests to be sent out from the lakeFS instance network. The feature should be enabled under `actions.lua.net_http_enabled` [configuration](../reference/configuration.md).
Request will time out after 30 seconds.

```lua
http.request(url [, body])
http.request{
  url = string,
  [method = string,]
  [headers = header-table,]
  [body = string,]
}
```

Returns a code (number), body (string), headers (table) and status (string).

 - code - status code number
 - body - string with the response body
 - headers - table with the response request headers (key/value or table of values)
 - status - status code text

The first form of the call will perform GET requests or POST requests if the body parameter is passed.

The second form accepts a table and allows you to customize the request method and headers.


Example of a GET request

```lua
local http = require("net/http")
local code, body = http.request("https://example.com")
if code == 200 then
    print(body)
else
    print("Failed to get example.com - status code: " .. code)
end

```

Example of a POST request

```lua
local http = require("net/http")
local code, body = http.request{
    url="https://httpbin.org/post",
    method="POST",
    body="custname=tester",
    headers={["Content-Type"]="application/x-www-form-urlencoded"},
}
if code == 200 then
    print(body)
else
    print("Failed to post data - status code: " .. code)
end
```
