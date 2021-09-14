BEGIN;

-- wild_stars returns T if pattern matches hay using IAM-style wildcards: *
-- is any string, ? is a single char.  It fails if pattern contains % or _
-- chars.
CREATE OR REPLACE FUNCTION pg_temp.wild_stars(pattern VARCHAR, hay VARCHAR)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE
        unsafe BOOLEAN;
	match BOOLEAN;

    BEGIN
        SELECT pattern LIKE '%\%%' OR pattern LIKE '%\_%' INTO STRICT unsafe;
	IF unsafe THEN
	    RAISE EXCEPTION 'unsafe pattern % contains "%%" or "_"', pattern;
	END IF;
        SELECT hay LIKE replace(replace(pattern, '*', '%'), '?', '_') INTO STRICT match;
	RETURN match;
    END;
$$;

-- jsonb_string translates a JSONB string object to SQL TEXT.  See
-- https://stackoverflow.com/a/58755595.
CREATE OR REPLACE FUNCTION pg_temp.jsonb_string(j JSONB) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
    SELECT j ->> 0
$$;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["fs:AttachStorageNamespace"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE id IN (
    SELECT DISTINCT id FROM (
        SELECT id, pg_temp.jsonb_string(s->'Effect') AS effect, jsonb_array_elements(s->'Action') AS action
	FROM (
	    SELECT id, jsonb_array_elements(statement) s FROM auth_policies
	    -- Update only statements that never mention
	    -- AttachStorageNamespace.  So downgrade can do nothing, and
	    -- re-upgrading will not re-add an existing statement or harm
	    -- pre-existing policies.
	    WHERE NOT jsonb_path_exists(statement, 'strict $[*].Action[*] ? (@ == "fs:AttachStorageNamespace")')
	) y
    ) x
    WHERE effect = 'allow' AND pg_temp.wild_stars(pg_temp.jsonb_string(action), 'fs:CreateRepository')
);

END;
