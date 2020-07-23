\set aid random(1, 1000000 * :scale)
\set tid random(1, 1000000 * :scale)
BEGIN;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir1/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '1', '1' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
END;
