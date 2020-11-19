update catalog_branches_export_state
set state='exported-successfully'
where state = 'export-repaired'