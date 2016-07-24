package  com.highperformancespark.examples.goldilocks

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object RDDJoinExamples {

 /* For Example, suppose we have one RDD with some data in the form (Panda id, score)
 and another RDD with (Panda id, address), and we want to send each Panda some mail
 with her best score. We could join the RDDs on ID and then compute the best score
 for each address. Like this:

  'ToDo: Insert Example'

  However, this is  slower than first reducing the score data, so that the
  //first dataset contains only one row for each Panda with her best score and then
   //joining that data with the address data.

  'ToDO: Insert an example of this' */
 //tag::joinScoresWithAddress[]
  def joinScoresWithAddress1( scoreRDD : RDD[(Long, Double)],
   addressRDD : RDD[(Long, String )]) : RDD[(Long, (Double, String))]= {
     val joinedRDD = scoreRDD.join(addressRDD)
      joinedRDD.reduceByKey( (x, y) => if(x._1 > y._1) x else y )
  }
  //end::joinScoresWithAddress[]

  //tag::leftOuterJoinScoresWithAddress[]
  def outerJoinScoresWithAddress( scoreRDD : RDD[(Long, Double)],
   addressRDD : RDD[(Long, String )]) : RDD[(Long, (Double, Option[String]))]= {
     val joinedRDD = scoreRDD.leftOuterJoin(addressRDD)
      joinedRDD.reduceByKey( (x, y) => if(x._1 > y._1) x else y )
 }
  //end::leftOuterJoinScoresWithAddress[]

  //tag::joinScoresWithAddressFast[]
  def joinScoresWithAddress2( scoreRDD : RDD[(Long, Double)],
    addressRDD : RDD[(Long, String )]) : RDD[(Long, (Double, String))]= {
    //stuff
   val bestScoreData = scoreRDD.reduceByKey((x, y) => if(x > y) x else y)
   bestScoreData.join(addressRDD)

  }
  //end::joinScoresWithAddressFast[]
/*
 We could make the example in the previous section even faster,
 by using the partitioner for the address data as an argument for
 the reduce by key step.
 'ToDO: Insert the code to show this here' */
  //tag::joinScoresWithAddress3[]
  def joinScoresWithAddress3( scoreRDD : RDD[(Long, Double)],
   addressRDD : RDD[(Long, String )]) : RDD[(Long, (Double, String))]= {
   //if addressRDD has a known partitioner we should use that,
   //otherwise it has a default hash parttioner, which we can reconstrut by getting the number of
   // partitions.
   val addressDataPartitioner = addressRDD.partitioner match {
    case (Some(p)) => p
    case (None) => new HashPartitioner(addressRDD.partitions.length)
   }
   val bestScoreData = scoreRDD.reduceByKey(addressDataPartitioner, (x, y) => if(x > y) x else y)
   bestScoreData.join(addressRDD)
  }
 //end::joinScoresWithAddress3[]

  def debugString( scoreRDD : RDD[(Long, Double)],
   addressRDD : RDD[(Long, String )])  = {
   //tag::debugString[]
   scoreRDD.join(addressRDD).toDebugString
   //end::debugString[]
  }

 /*
  *  Suppose we had two datasets of information about each panda,
  *  one with the scores, and one with there favorite foods.
  *  We could use cogroup to associate each Pandas id with an iterator
  *  of their scores and another iterator of their favorite foods.
  */


 def coGroupExample( scoreRDD : RDD[(Long, Double)], foodRDD : RDD[(Long, String )],
  addressRDD : RDD[(Long, String )])  = {
  //tag::coGroupExample1[]
   val cogroupedRDD: RDD[(Long, (Iterable[Double], Iterable[String]))] = scoreRDD.cogroup(foodRDD)
  //end::coGroupExample1[]

  /*
   * For example, if we needed to join the panda score data with both address
   * and favorite foods, it would be better to use co group than two
   * join operations.
   */

  //tag::coGroupExample2[]
  val addressScoreFood = addressRDD.cogroup(scoreRDD, foodRDD)
  //end::coGroupExample2[]
   }
 }
