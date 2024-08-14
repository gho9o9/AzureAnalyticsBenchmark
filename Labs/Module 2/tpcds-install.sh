#!/bin/bash
sudo apt update
sudo apt-get --assume-yes install gcc make flex bison byacc git
cd /usr/local/bin
sudo git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
sudo make OS=LINUX
