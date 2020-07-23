\set aid random(1, 1000000 * :scale)
\set tid random(1, 1000000 * :scale)
BEGIN;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir50/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '50', '50' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir49/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '49', '49' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir48/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '48', '48' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir47/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '47', '47' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir46/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '46', '46' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir45/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '45', '45' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir44/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '44', '44' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir43/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '43', '43' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir42/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '42', '42' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir41/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '41', '41' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir40/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '40', '40' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir39/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '39', '39' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir38/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '38', '38' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir37/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '37', '37' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir36/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '36', '36' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir35/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '35', '35' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir34/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '34', '34' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir33/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '33', '33' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir32/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '32', '32' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir31/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '31', '31' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir30/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '30', '30' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir29/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '29', '29' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir28/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '28', '28' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir27/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '27', '27' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir26/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '26', '26' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir25/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '25', '25' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir24/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '24', '24' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir23/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '23', '23' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir22/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '22', '22' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir21/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '21', '21' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir20/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '20', '20' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir19/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '19', '19' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir18/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '18', '18' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir17/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '17', '17' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir16/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '16', '16' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir15/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '15', '15' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir14/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '14', '14' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir13/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '13', '13' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir12/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '12', '12' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata)
      VALUES (1,'dir11/72070CE8B8E843E4AAE3ADB67A4C7E60' || :tid || '/file_1' || :tid || '_' || :aid || '11', '11' || '5e30825ac' || to_hex(:aid),'55ab9ce2' || to_hex(:aid), :aid, NULL)
      ON CONFLICT (branch_id,path,min_commit)
      DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, max_commit=max_commit_id()
      RETURNING ctid;
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
