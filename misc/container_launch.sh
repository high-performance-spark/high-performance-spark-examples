#!/bin/bash
if [ ! -f /high-performance-spark-examples/iceberg-workshop/Workshop.ipynb ]; then
  cp /high-performance-spark-examples/iceberg-workshop-solutions/Workshop-Template.ipynb /high-performance-spark-examples/iceberg-workshop/Workshop.ipynb
fi
jupyter-lab --ip 0.0.0.0 --port 8877
