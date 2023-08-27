package com.highperformancespark.examples.transformations

import org.apache.spark.rdd.RDD
class SmartAggregations {

  //tag::naiveAggregation[]
  /**
   * Given an RDD of (PandaInstructor, ReportCardText) aggregate by instructor
   * to an RDD of distinct keys of (PandaInstructor, ReportCardStatistics)
   * where ReportCardMetrics is a case class with
   *
   * longestWord -> The longest word in all of the reports written by this instructor
   * happyMentions -> The number of times this intructor mentioned the word happy
   * averageWords -> The average number of words per report card for this instructor
   */
  def calculateReportCardStatistics(rdd : RDD[(String, String)]
  ): RDD[(String, ReportCardMetrics)] ={

    rdd.aggregateByKey(new MetricsCalculator(totalWords = 0,
      longestWord = 0, happyMentions = 0, numberReportCards = 0))(
      seqOp = ((reportCardMetrics, reportCardText) =>
        reportCardMetrics.sequenceOp(reportCardText)),
      combOp = (x, y) => x.compOp(y))
    .mapValues(_.toReportCardMetrics)
  }
  //end::naiveAggregation[]


  /**
    * Same as above, but rather than using the 'MetricsCalculator' class for
   * computing the aggregations functions, we use a modified implementation
   * called 'MetricsCalculatorReuseObjects' which modifies the original
   * accumulator and returns it for both the sequnece op and the aggregatio op.
   *
   * @param rdd
   * @return
   */
  def calculateReportCardStatisticsReuseObjects(rdd : RDD[(String, String)]
  ): RDD[(String, ReportCardMetrics)] ={

    rdd.aggregateByKey(new MetricsCalculatorReuseObjects(totalWords = 0,
      longestWord = 0, happyMentions = 0, numberReportCards = 0))(
      seqOp = (reportCardMetrics, reportCardText) =>
        reportCardMetrics.sequenceOp(reportCardText),
      combOp = (x, y) => x.compOp(y))
    .mapValues(_.toReportCardMetrics)
  }

  //tag::goodAggregation[]
  def calculateReportCardStatisticsWithArrays(rdd : RDD[(String, String)]
  ): RDD[(String, ReportCardMetrics)] = {

    rdd.aggregateByKey(
      //the zero value is a four element array of zeros
      Array.fill[Int](4)(0)
    )(
    //seqOp adds the relevant values to the array
      seqOp = (reportCardMetrics, reportCardText) =>
        MetricsCalculator_Arrays.sequenceOp(reportCardMetrics, reportCardText),
    //combo defines how the arrays should be combined
      combOp = (x, y) => MetricsCalculator_Arrays.compOp(x, y))
    .mapValues(MetricsCalculator_Arrays.toReportCardMetrics)
  }
  //end::goodAggregation[]

}
//tag::caseClass[]
case class ReportCardMetrics(
  longestWord : Int,
  happyMentions : Int,
  averageWords : Double)
//end::caseClass[]


//tag::firstCalculator[]
 class MetricsCalculator(
  val totalWords : Int,
  val longestWord: Int,
  val happyMentions : Int,
  val numberReportCards: Int) extends Serializable {

  def sequenceOp(reportCardContent : String) : MetricsCalculator = {
    val words = reportCardContent.split(" ")
    val tW = words.length
    val lW = words.map( w => w.length).max
    val hM = words.count(w => w.toLowerCase.equals("happy"))

    new MetricsCalculator(
      tW + totalWords,
      Math.max(longestWord, lW),
      hM + happyMentions,
      numberReportCards + 1)
  }

   def compOp(other : MetricsCalculator) : MetricsCalculator = {
     new MetricsCalculator(
       this.totalWords + other.totalWords,
       Math.max(this.longestWord, other.longestWord),
       this.happyMentions + other.happyMentions,
       this.numberReportCards + other.numberReportCards)
   }

   def toReportCardMetrics =
     ReportCardMetrics(
       longestWord,
       happyMentions,
       totalWords.toDouble/numberReportCards)
}
//end::firstCalculator[]

//tag::calculator_reuse[]
class MetricsCalculatorReuseObjects(
  var totalWords : Int,
  var longestWord: Int,
  var happyMentions : Int,
  var numberReportCards: Int) extends Serializable {

  def sequenceOp(reportCardContent : String) : this.type = {
    val words = reportCardContent.split(" ")
    totalWords += words.length
    longestWord = Math.max(longestWord, words.map( w => w.length).max)
    happyMentions += words.count(w => w.toLowerCase.equals("happy"))
    numberReportCards +=1
    this
  }

  def compOp(other : MetricsCalculatorReuseObjects) : this.type = {
    totalWords += other.totalWords
    longestWord = Math.max(this.longestWord, other.longestWord)
    happyMentions += other.happyMentions
    numberReportCards += other.numberReportCards
    this
  }

  def toReportCardMetrics =
    ReportCardMetrics(
      longestWord,
      happyMentions,
      totalWords.toDouble/numberReportCards)
}
//end::calculator_reuse[]


//tag::calculator_array[]
object MetricsCalculator_Arrays extends Serializable {
  val totalWordIndex = 0
  val longestWordIndex = 1
  val happyMentionsIndex = 2
  val numberReportCardsIndex = 3

  def sequenceOp(reportCardMetrics : Array[Int],
    reportCardContent : String) : Array[Int] = {

    val words = reportCardContent.split(" ")
    //modify each of the elements in the array
    reportCardMetrics(totalWordIndex) += words.length
    reportCardMetrics(longestWordIndex) = Math.max(
      reportCardMetrics(longestWordIndex),
      words.map(w => w.length).max)
    reportCardMetrics(happyMentionsIndex) += words.count(
      w => w.toLowerCase.equals("happy"))
    reportCardMetrics(numberReportCardsIndex) +=1
    reportCardMetrics
  }

  def compOp(x : Array[Int], y : Array[Int]) : Array[Int] = {
    //combine the first and second arrays by modifying the elements
    // in the first array
    x(totalWordIndex)  += y(totalWordIndex)
    x(longestWordIndex) = Math.max(x(longestWordIndex), y(longestWordIndex))
    x(happyMentionsIndex) += y(happyMentionsIndex)
    x(numberReportCardsIndex) += y(numberReportCardsIndex)
    x
  }

  def toReportCardMetrics(ar : Array[Int]) : ReportCardMetrics =
    ReportCardMetrics(
      ar(longestWordIndex),
      ar(happyMentionsIndex),
      ar(totalWordIndex)/ar(numberReportCardsIndex)
    )
}
//end::calculator_array[]


object CollectionRoutines{

  //tag::implicitExample[]
  def findWordMetrics[T <:Seq[String]](collection : T ): (Int, Int)={
    val iterator = collection.toIterator
    var mentionsOfHappy = 0
    var longestWordSoFar = 0
    while(iterator.hasNext){
      val n = iterator.next()
      if(n.toLowerCase == "happy"){
        mentionsOfHappy +=1
      }
      val length = n.length
      if(length> longestWordSoFar) {
        longestWordSoFar = length
      }

    }
    (longestWordSoFar, mentionsOfHappy)
  }
  //end::implicitExample[]


  //tag::fasterSeqOp[]
  val totalWordIndex = 0
  val longestWordIndex = 1
  val happyMentionsIndex = 2
  val numberReportCardsIndex = 3
  def fasterSeqOp(reportCardMetrics : Array[Int], content  : String): Array[Int] = {
    val words: Seq[String] = content.split(" ")
    val (longestWord, happyMentions) = CollectionRoutines.findWordMetrics(words)
    reportCardMetrics(totalWordIndex) += words.length
    reportCardMetrics(longestWordIndex) = longestWord
    reportCardMetrics(happyMentionsIndex) += happyMentions
    reportCardMetrics(numberReportCardsIndex) +=1
    reportCardMetrics
  }
  //end::fasterSeqOp[]
}
