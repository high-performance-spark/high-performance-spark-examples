# high-performance-spark-examples
Examples for High Performance Spark

We are in the progress of updata this for Spark 3.3+ and the 2ed edition of our book!

# Building

Most of the examples can be built with sbt, the C and Fortran components depend on gcc, g77, and cmake.

# Tests

The full test suite depends on having the C and Fortran components built as well as a local R installation available.

The most "accuate" way of seeing how we run the tests is to look at the .github workflows

# History Server

The history server can be a great way to figure out what's going on. You can set the SPARK_EVENTLOG=true before running the scala tests and you'll get the history server too!
