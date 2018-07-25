#!/bin/bash

set -e

if hostname | grep "hn0-" ; then
    sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/gkanade-hdfsrackaware/hdfsrackaware/makehdfsrackaware.py
    sudo python makehdfsrackaware.py
else
    echo "not hn0" | logger
fi
