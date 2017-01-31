# This script triggers a number of different PySpark errors

from pyspark import *
from pyspark.sql.session import SparkSession

global sc

def nonExistantInput(sc):
    """
    Attempt to load non existant input
    >>> nonExistantInput(sc)
    Traceback (most recent call last):
        ...
    Py4JJavaError:...
    """
    # tag::nonExistant[]
    failedRdd = sc.textFile("file:///doesnotexist")
    failedRdd.count()
    # end::nonExistant[]

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

def runOutOfMemory(sc):
    """
    Run out of memory on the workers.
    >>> runOutOfMoery(sc)
    boop
    """
    # tag::worker_oom[]
    data = sc.parallelize(range(10))
    def generate_too_much(itr):
        return range(10000000000000000000000)
    itr = data.flatMap(generate_too_much)
    itr.count()
    # end::worker_oom[]

def _setupTest():
    globs = globals()
    spark = SparkSession.builder \
                        .master("local[4]") \
                        .getOrCreate()
    sc = spark._sc
    globs['sc'] = sc
    return globs
    
def _test():
    """
    Run the tests. 
    Note this will print a lot of error message to stderr since we don't capture the JVM sub process
    stdout/stderr for doctests.
    """
    import doctest
    globs = setupTest()
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

import sys
if __name__ == "__main__":
    _test()
# Hack to support running in nose
elif sys.stdout != sys.__stdout__:
    _setupTest()
else:
    1 / 0
