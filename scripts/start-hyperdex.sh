#!/bin/bash

killall -9 replicant-daemon
killall -9 hyperdex-daemon

rm ./daemons -rf

mkdir -p ./daemons/coordinator
mkdir -p ./daemons/daemon1

hyperdex coordinator -d -D $(readlink -f ./daemons/coordinator) -l 127.0.0.1
sleep 1s
hyperdex daemon -f -D $(readlink -f ./daemons/daemon1) -c 127.0.0.1 -P 1982 -l 127.0.0.1 -p 2001 -t 1
popd

# hyperdex daemon -d -D ./daemons/daemon2 -c 127.0.0.1 -P 1982 -l 127.0.0.1 -p 2002 -t 1
# hyperdex daemon -d -D ./daemons/daemon3 -c 127.0.0.1 -P 1982 -l 127.0.0.1 -p 2003 -t 1
# hyperdex daemon -d -D ./daemons/daemon4 -c 127.0.0.1 -P 1982 -l 127.0.0.1 -p 2004 -t 1
