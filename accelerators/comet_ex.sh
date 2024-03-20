#!/bin/bash
set -ex

SPARK_MAJOR=3.4
export SPARK_MAJOR

source setup_comet.sh
source comet_env_setup.sh
cd ..
USE_COMET="true" ./run_sql_examples.sh
