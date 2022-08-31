#!/bin/bash -e

sh /opt/starrocks/be/bin/start_be.sh --daemon
sleep 5
sh /opt/starrocks/fe/bin/start_fe.sh

