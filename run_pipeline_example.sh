#!/bin/bash
set -ex
cd spark-pipeline-ex/
spark-pipelines run --spec spark-pipeline.yaml
