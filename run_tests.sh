#!/bin/bash

# Copyright (c) 2018, Postgres Professional

set -eux

echo CHECK_CODE=$CHECK_CODE

status=0

# perform code analysis if necessary
if [ "$CHECK_CODE" = "clang" ]; then
    scan-build --status-bugs make USE_PGXS=1 || status=$?
    exit $status
fi

# don't forget to "make clean"
make USE_PGXS=1 clean

# initialize database
initdb

# build extension
make USE_PGXS=1 install

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_pageprep'" >> $PGDATA/postgresql.conf
echo "pg_pageprep.enable_workers = off" >> $PGDATA/postgresql.conf
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w

# check startup
status=$?
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
PGPORT=55435 make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

exit $status
