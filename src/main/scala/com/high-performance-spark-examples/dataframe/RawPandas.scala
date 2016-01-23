package com.highperformancespark.examples.dataframe
/**
 * @param id panda id
 * @param zip zip code of panda residence
 * @param happy if panda is happy
 * @param attributes array of panada attributes
 */
case class RawPanda(id: Long, zip: String, happy: Boolean, attributes: Array[Double])
