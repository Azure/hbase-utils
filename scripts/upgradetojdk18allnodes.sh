#!/bin/sh

sudo add-apt-repository ppa:openjdk-r/ppa -y && sudo apt-get -y update && sudo apt-get install -y openjdk-8-jdk
op=$(ls /usr/lib/jvm)
echo "Msg " $op | logger