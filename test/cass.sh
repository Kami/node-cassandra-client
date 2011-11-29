#!/bin/bash

basedir=`dirname $0`

export CASSANDRA_CONF=$basedir/conf/

rm -rf /tmp/cass/*

exec /opt/cassandra/bin/cassandra -f
