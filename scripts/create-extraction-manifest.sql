--  Add New Scripts
-- PostgreSQL script to create a manifest file for extracting files
-- from LakeFS to S3.  Use this manifest to run treeverse-distcp,
-- which will extract the files.

-- -- -- -- -- -- -- -- --
-- Set these variables by running "psql --var VARIABLE=VALUE", e.g.:
--
--     "psql -var repository_name=foo -var branch_name=master --var dst_bucket_name=foo-extract".

-- Variable repository_name: repository to be extracted.  Must be
-- specified.
--
-- Variable branch_name: branch to be extracted.  Must be specified
-- (otherwise object paths can identify more than a single object).
--
-- Variable dst_bucket_name: name of bucket to place files.  Must be
-- specified.

-- Avoid superfluous output (such as "CREATE FUNCTION)
\set QUIET 1

CREATE FUNCTION pg_temp.maybe_concat_slash(p text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT regexp_replace(p, '/$', '') || '/';
$$;

CREATE FUNCTION pg_temp.join_paths(p text, q text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT pg_temp.maybe_concat_slash(p) || q;
$$;

-- encode URI from https://stackoverflow.com/a/60260190/192263
CREATE FUNCTION pg_temp.encode_uri_component(text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT string_agg(
      CASE WHEN bytes > 1 OR c !~ '[0-9a-zA-Z_.!~*''()-]+' THEN
	    regexp_replace(encode(convert_to(c, 'utf-8')::bytea, 'hex'), '(..)', E'%\\1', 'g')
      ELSE
            c
      END,
      ''
   )
   FROM (
       SELECT c, octet_length(c) bytes
       FROM regexp_split_to_table($1, '') c
   ) q;
$$;

-- Return the first part of path.
CREATE FUNCTION pg_temp.get_head(path text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT regexp_replace($1, '^s3://([^/]*)/.*$', '\1')
$$;

-- Return the bucket name of path: its head if it has slashes, or all
-- of it if it does not.
CREATE FUNCTION pg_temp.get_bucket(path text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT CASE WHEN head = '' THEN $1 ELSE head END FROM (
      SELECT pg_temp.get_head(path) head
   ) i;
$$;

-- If path is an S3 path with a key after the bucket, return the rest
-- ("path") of path (everything after the first slash) and a trailing
-- slash.  Otherwise return ''.
CREATE FUNCTION pg_temp.get_rest(path text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT CASE WHEN tail = '' THEN '' ELSE pg_temp.maybe_concat_slash(tail) END FROM (
      SELECT substr($1, length(pg_temp.get_head($1)) + 7) tail
   ) i;
$$;

-- Format output appropriately
\pset format csv
\pset tuples_only on

-- TODO(ariels): Works just for S3-based namespaces.  Current
-- alternatives (mem, local) do not require support, future may be
-- different.
SELECT
DISTINCT ON (physical_address)
     regexp_replace(pg_temp.get_bucket(storage_namespace), '^s3://', 'arn:aws:s3:::') src_bucket_arn,
     pg_temp.encode_uri_component(json_build_object(
	 'dstBucket', :'dst_bucket_name',
	 'dstKey', pg_temp.join_paths(:'repository_name', path),
	 'srcKey', concat(pg_temp.get_rest(storage_namespace), physical_address)) #>> '{}')
FROM (
    SELECT entry.path path, entry.physical_address physical_address, entry.min_commit min_commit,
    	   entry.max_commit = 0 tombstone, -- true for an uncommitted deletion
           repository.storage_namespace storage_namespace
    FROM (catalog_entries entry
          JOIN catalog_branches branch ON entry.branch_id = branch.id
          JOIN catalog_repositories repository ON branch.repository_id = repository.id)
    WHERE repository.name = :'repository_name' AND
          branch.name = :'branch_name' AND
          -- uncommitted        OR                          current
          (entry.min_commit = 0 OR entry.min_commit > 0 AND entry.max_commit = catalog_max_commit_id()) AND
          -- Skip explicit physical addresses: imported from elsewhere
          -- with meaningful name so do not export
          regexp_match(entry.physical_address, '^[a-zA-Z0-9]+://') IS NULL
) i
WHERE NOT tombstone
ORDER BY physical_address, min_commit;

