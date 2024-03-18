#!/bin/bash

set -ex
source setup_comet.sh
source comet_env_setup.sh
USE_COMET="true" ../run_sql_examples.sh
