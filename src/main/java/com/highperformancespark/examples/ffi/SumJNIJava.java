package com.highperformancespark.examples.ffi;

// tag::sumJNIJava[]
class SumJNIJava {
  public static native Integer sum(Integer[] array);
}
// end::sumJNIJava[]
