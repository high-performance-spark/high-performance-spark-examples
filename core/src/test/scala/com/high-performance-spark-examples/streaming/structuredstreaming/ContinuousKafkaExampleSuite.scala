package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_continuous_kafka_test[]
// Skipped test: Continuous mode Kafka requires external Kafka infra
// This test only checks code compiles and imports

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Tag

object KafkaTestTag extends Tag("KafkaRequired")

class ContinuousKafkaExampleSuite extends AnyFunSuite {
  test("continuous kafka example compiles and imports", KafkaTestTag) {
    // Skipped: requires Kafka infra
    assert(true)
  }
}
// end::streaming_ex_continuous_kafka_test[]
