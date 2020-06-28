-- PostgreSQL script to create a manifest file for extracting files
-- from LakeFS to S3.  Use this manifest to run treeverse-distcp,
-- which will extract the files.

-- -- -- -- -- -- -- -- --
-- Set these variables by running "psql --var VARIABLE=VALUE", e.g.:
--
--     "psql -var branch_name=master".

-- Variable branch_name: branch to be extracted.  Must be specified
-- (otherwise object paths can identify more than a single object).
--
-- Variable dst_bucket_name: name of bucket to place files.  Must be
-- specified.

CREATE FUNCTION pg_temp.join_paths(p text, q text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
   SELECT regexp_replace(p, '/$', '') || '/' || q;
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

-- Format output appropriately
\pset format csv
\pset tuples_only on

-- TODO(ariels): Works just for S3-based namespaces.  Current
-- alternatives (mem, local) do not require support, future may be
-- different.
SELECT regexp_replace(repository.storage_namespace, '^s3://', 'arn:aws:s3:::') src_bucket_arn,
     pg_temp.encode_uri_component(json_build_object(
	 'dstBucket', :'dst_bucket_name',
	 'dstPath', pg_temp.join_paths(repository.name, entry.path),
	 'srcPath', entry.physical_address) #>> '{}')
FROM (lakefs_catalog.entries_lineage_full_v entry
      JOIN lakefs_catalog.branches branch
      	   ON entry.displayed_branch = branch.id
      JOIN lakefs_catalog.repositories repository
      	   ON branch.repository_id = repository.id)
WHERE entry.rank=1 AND branch.name = :'branch_name';
