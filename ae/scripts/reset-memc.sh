#!/bin/bash

addr="10.0.2.181"
port="23333"

# kill old
# ssh ${addr} "pkill -f memcached; memcached -u root -l ${addr} -p ${port} -c 10000 -d -P /tmp/memcached.pid"
ssh ${addr} "cat /tmp/memcached.pid | xargs kill > /dev/null 2>&1; sleep 1; memcached -u root -l ${addr} -p ${port} -c 10000 -d -P /tmp/memcached.pid"
ssh ${addr} "cat /tmp/memcached.pid | xargs kill > /dev/null 2>&1; sleep 1; memcached -u root -l ${addr} -p ${port} -c 10000 -d -P /tmp/memcached.pid"

sleep 1

# init
echo -e "set nr_nodes 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
