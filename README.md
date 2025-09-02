# high-performance-spark-examples
Examples for High Performance Spark

We are in the progress of updata this for Spark 4 (some parts depending on external libraries like Iceberg, Comet, etc. are still 3.X) and the 2ed edition of our book!

# Building

Most of the examples can be built with sbt, the C and Fortran components depend on gcc, g77, and cmake.

# Tests

The full test suite depends on having the C and Fortran components built as well as a local R installation available.

The most "accuate" way of seeing how we run the tests is to look at the .github workflows

# History Server

The history server can be a great way to figure out what's going on.

By default the history server writes to `/tmp/spark-events` so you'll need to create that directory if not setup with

`mkdir -p /tmp/spark-events`

The scripts for running the examples generally run with the event log enabled.

You can set the SPARK_EVENTLOG=true before running the scala tests and you'll get the history server too!

e.g.

`SPARK_EVENTLOG=true sbt test`

If you want to run just a specific test you can run [testOnly](https://www.scala-sbt.org/1.x/docs/Testing.html)

Then to view the history server you'll want to launch it using the `${SPARK_HOME}/sbin/start-history-server.sh` then you [can go to your local history server](http://localhost:18080/)
