package com.highperformancespark.examples.dataframe

import java.util.Arrays
import java.util.Objects

/**
 * @param id panda id
 * @param zip zip code of panda residence
 * @param pt Type of panda as a string
 * @param happy if panda is happy
 * @param attributes array of panada attributes
 */
case class RawPanda(id: Long, zip: String, pt: String,
  happy: Boolean, attributes: Array[Double]) {
  override def equals(o: Any) = o match {
    case other: RawPanda => (id == other.id && pt == other.pt &&
        happy == other.happy && attributes.deep == other.attributes.deep)
    case _ => false
  }
  override def hashCode(): Int = {
    3 * Objects.hashCode(id) + 7 * Objects.hashCode(zip) +
    11 * Objects.hashCode(pt) + 13 * Arrays.hashCode(attributes)
  }
}

/**
 * @param name place name
 * @param pandas pandas in that place
 */
case class PandaPlace(name: String, pandas: Array[RawPanda])

case class CoffeeShop(zip: String, name: String)
