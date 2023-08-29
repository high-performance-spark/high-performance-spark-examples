# This script triggers a number of different PySpark errors

from pyspark.sql.session import SparkSession
import sys

global sc


def nonExistentInput(sc):
    """
    Attempt to load non existent input
    >>> nonExistentInput(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    # tag::nonExistent[]
    failedRdd = sc.textFile("file:///doesnotexist")
    failedRdd.count()
    # end::nonExistent[]


def throwOuter(sc):
    """
    Attempt to load non existant input
    >>> throwOuter(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    # tag::throwOuter[]
    data = sc.parallelize(range(10))
    transform1 = data.map(lambda x: x + 1)
    transform2 = transform1.map(lambda x: x / 0)
    transform2.count()
    # end::throwOuter[]


def throwInner(sc):
    """
    Attempt to load non existant input
    >>> throwInner(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    # tag::throwInner[]
    data = sc.parallelize(range(10))
    transform1 = data.map(lambda x: x / 0)
    transform2 = transform1.map(lambda x: x + 1)
    transform2.count()
    # end::throwInner[]


# tag::rewrite[]
def add1(x):
    """
    Add 1
    >>> add1(2)
    3
    """
    return x + 1


def divZero(x):
    """
    Divide by zero (cause an error)
    >>> divZero(2)
    Traceback (most recent call last):
        ...
    ZeroDivisionError: integer division or modulo by zero
    """
    return x / 0


def throwOuter2(sc):
    """
    Attempt to load non existant input
    >>> throwOuter2(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    data = sc.parallelize(range(10))
    transform1 = data.map(add1)
    transform2 = transform1.map(divZero)
    transform2.count()


def throwInner2(sc):
    """
    Attempt to load non existant input
    >>> throwInner2(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    data = sc.parallelize(range(10))
    transform1 = data.map(divZero)
    transform2 = transform1.map(add1)
    transform2.count()


# end::rewrite[]


def throwInner3(sc):
    """
    Attempt to load non existant input
    >>> throwInner3(sc)
    Reject 10
    """
    data = sc.parallelize(range(10))
    rejectedCount = sc.accumulator(0)

    def loggedDivZero(x):
        import logging

        try:
            return [x / 0]
        except Exception as e:
            rejectedCount.add(1)
            logging.warning("Error found " + repr(e))
            return []

    transform1 = data.flatMap(loggedDivZero)
    transform2 = transform1.map(add1)
    transform2.count()
    print("Reject " + str(rejectedCount.value))


def runOutOfMemory(sc):
    """
    Run out of memory on the workers.
    In standalone modes results in a memory error, but in YARN may trigger YARN container
    overhead errors.
    >>> runOutOfMemory(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    # tag::worker_oom[]
    data = sc.parallelize(range(10))

    def generate_too_much(itr):
        return range(10000000000000)

    itr = data.flatMap(generate_too_much)
    itr.count()
    # end::worker_oom[]


def _setupTest():
    globs = globals()
    spark = SparkSession.builder.master("local[4]").getOrCreate()
    sc = spark._sc
    globs["sc"] = sc
    return globs


def _test():
    """
    Run the tests.
    Note this will print a lot of error message to stderr since we don't capture the JVM sub process
    stdout/stderr for doctests.
    """
    import doctest

    globs = _setupTest()
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS
    )
    print("All tests done, stopping Spark context.")
    globs["sc"].stop()
    if failure_count:
        exit(-1)
    else:
        exit(0)


if __name__ == "__main__":
    _test()
# Hack to support running in nose
elif sys.stdout != sys.__stdout__:
    _setupTest()
