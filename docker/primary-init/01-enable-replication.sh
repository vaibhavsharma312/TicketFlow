#!/bin/bash
# Runs once, on first init of the primary.
# Allows the replica container to open replication connections.
set -e
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
echo "[init] replication access enabled in pg_hba.conf"
