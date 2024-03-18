#!/bin/bash
set -ex

sudo apt-get update
sudo apt-get install -y locales wget tar tzdata git ccache cmake ninja-build build-essential llvm-dev clang libiberty-dev libdwarf-dev libre2-dev libz-dev libssl-dev libboost-all-dev libcurl4-openssl-dev maven rapidjson-dev libdouble-conversion-dev libgflags-dev libsodium-dev libsnappy-dev nasm && sudo apt install -y libunwind-dev
sudo apt-get install -y libgoogle-glog-dev
sudo apt-get -y install docker-compose
sudo apt-get install -y libre2-9
