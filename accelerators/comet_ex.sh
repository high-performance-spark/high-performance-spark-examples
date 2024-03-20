#!/bin/bash
set -ex

SPARK_MAJOR=3.4
SPARK_VERSION=3.4.2
export SPARK_MAJOR
export SPARK_VERSION

source setup_comet.sh
source comet_env_setup.sh
cd ..
USE_COMET="true" ./run_sql_examples.sh
