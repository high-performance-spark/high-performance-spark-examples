#!/bin/bash
set -ex

SPARK_MAJOR=3.4
SPARK_VERSION=3.4.2
export SPARK_MAJOR
export SPARK_VERSION

source setup_comet.sh
pushd ..
source ./env_setup.sh 
popd
source comet_env_setup.sh
pushd ..
USE_COMET="true" ./run_sql_examples.sh
