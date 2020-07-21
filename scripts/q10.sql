\set aid random(1, 1000000 * :scale)
\set tid random(1, 1000000 * :scale)
BEGIN;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir10/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '10', '10' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir9/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '9', '9' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir8/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '8', '8' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir7/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '7', '7' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir6/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '6', '6' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir5/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '5', '5' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir4/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '4', '4' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir3/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '3', '3' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir2/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '2', '2' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir1/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '1', '1' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
END;
