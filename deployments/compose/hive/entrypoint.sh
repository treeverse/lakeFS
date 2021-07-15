#!/bin/bash

[[ -n "${DB_URI}" ]] && wait-for ${DB_URI}

[[ ! -f initSchema.completed ]] && schematool -dbType mysql -initSchema && touch initSchema.completed

hive --service metastore
