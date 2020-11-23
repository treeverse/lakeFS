UPDATE catalog_branches_export_state
SET state='exported-successfully'
WHERE state = 'export-repaired';
