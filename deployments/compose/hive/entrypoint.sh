#!/bin/bash

for i in `seq 30`; do
    mysqladmin --user=admin --password=admin  --host=mariadb  ping && break
    echo Still waiting for mariadb...
    sleep 2
done
# Try to run hive even if failed to ping mariadb -- maybe we'll get lucky!

[[ ! -f initSchema.completed ]] && schematool -dbType mysql -initSchema && touch initSchema.completed

hive --service metastore
