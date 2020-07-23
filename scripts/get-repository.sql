SELECT r.name, r.storage_namespace, b.name AS default_branch, r.creation_date
		 	FROM repositories r, branches b
		 	WHERE r.id = b.repository_id AND r.default_branch = b.id AND r.name = 'repo1'
