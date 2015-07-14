#!/bin/bash

killall -9 replicant-daemon
killall -9 hyperdex-daemon

rm -rf /tmp/hyhac-testing
mkdir -p /tmp/hyhac-testing
cd /tmp/hyhac-testing

rm ./daemons -rf

mkdir -p ./daemons/coordinator
mkdir -p ./daemons/daemon1
mkdir -p ./daemons/daemon2
mkdir -p ./daemons/daemon3
mkdir -p ./daemons/daemon4
mkdir -p ./daemons/daemon5

hyperdex coordinator -d -D ./daemons/coordinator/ -l 127.0.0.1
sleep 1s
hyperdex daemon -f -D ./daemons/daemon1/ -c 127.0.0.1 -P 1982 -l 127.0.0.1 -p 2001 -t 1
