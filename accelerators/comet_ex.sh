#!/bin/bash
set -ex

# If you change this update the workflow version too.
SPARK_MAJOR=${SPARK_MAJOR:-3.5}
SPARK_VERSION=${SPARK_MAJOR}.1
export SPARK_MAJOR
export SPARK_VERSION

source setup_comet.sh
pushd ..
source ./env_setup.sh 
popd
source comet_env_setup.sh
pushd ..
USE_COMET="true" ./run_sql_examples.sh
