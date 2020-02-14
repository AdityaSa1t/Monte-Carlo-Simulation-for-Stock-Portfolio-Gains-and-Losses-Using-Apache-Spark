import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.functions.{col, desc, lead, stddev_pop}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.math.sqrt
import scala.collection.mutable.ListBuffer
import scala.io.Source

object SingleSimulation { //Runs a Monte-Carlo simulation for the given stock

  val log: Logger = LoggerFactory.getLogger(Driver.getClass)
  val allProfits = ListBuffer[Double]()

  def run(outpath:String, url:String): Unit = { // Method which runs the actual simulation

    //Setting up spark
    val spark = SparkSession.builder
      .master("local")
      .appName("SparkOne")
      .getOrCreate

    val window = Window.orderBy("timestamp")
    val nextCol = lead("close",1).over(window)
    val diff = col("close")-col("diff")


    //Getting result from the REST API
    var fileCsvUrl = Source.fromURL(url).mkString.stripMargin.lines.toList

    import spark.implicits._

    //Fitting REST API response into a dataframe
    val csvData: Dataset[String] = spark.sparkContext.parallelize(fileCsvUrl).toDS()
    val inputStock = spark.read.option("header", "true").option("mode","DROPMALFORMED").csv(csvData).drop("open","high","low","volume").withColumn("diff",nextCol)
      .withColumn("percent",diff).drop("diff")
    inputStock.show()

    /*val inputStock = spark.read
      .format("csv")
      .option("header","true")
      .option("mode","DROPMALFORMED")
      .load(inpath)
      .drop("open","high","low","volume").withColumn("diff",nextCol)
      .withColumn("percent",diff).drop("diff")*/

    val res = endDataFrame(spark,inputStock) // pass a dataframe to get the maximised profit for each simulation of the given stock
    res.show()

    log.debug("Maximum profit for each simulation:"+allProfits)

    //converting result list into a dataframe, which will be converted to a csv file
    import spark.implicits._
    val profits_df = allProfits.toList.toDF()
    profits_df.show()

    val outputConfig : Map[String, String] = Map(("delimiter", ","), ("header", "true"))

    log.debug("Saving results in "+ outpath +" after processing: "+url)
    profits_df.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .options(outputConfig)
      .csv(outpath)
  }

  def getProfit(lb: ListBuffer[Double]):Unit ={
    //basic function to maximize the profit in a given list of varying stock prices across a certain number of days.

    var prft: Double = 0
    var i: Int = 0

    var minprice = Integer.MAX_VALUE.toDouble
    while ( {
      i < lb.length
    }) {
      if (lb(i) < minprice) minprice = lb(i)
      else if (lb(i) - minprice > prft) prft = lb(i) - minprice
      log.debug("****In run "+i+" profit="+prft+"****")

      {
        i += 1; i - 1
      }
    }

    allProfits += prft

  }


  def getList(inputStock:DataFrame): ListBuffer[Double] = { //generates the random point from a seed/start value
    val closeAmnts = inputStock.select("close").sort(desc("close")).collect()

    val start = closeAmnts(0)(0).toString.toDouble
    log.info("Starting price of stock="+start)

    //get standard deviation
    val x = inputStock.agg(stddev_pop("percent"))


    val count = inputStock.count().toString.toDouble

    val sd = x.collect()(0)(0).toString.toDouble
    log.debug("Standard deviation="+sd)

    val volatility = sd * sqrt(count)

    val r :ThreadLocalRandom = ThreadLocalRandom.current()
    val rndm = r.nextDouble(0,sd)

    val predict = start+ r.nextDouble(0,volatility)

    val historyPoints= ListBuffer[Double]()

    historyPoints += predict

    for(index<- 1 until count.toInt-2){
      val point = start + (r.nextDouble(0,volatility)*(r.nextDouble(0,sd)))
      historyPoints += point
    }

    log.info("start ="+start+"\n\n\n standard deviation ="+sd+"\n\n\n\n volatility="+volatility+"\n\n\n\n"+"random value="+rndm+"\n\n\npredicted price="+predict)

    val profit = getProfit(historyPoints)
    log.info("Profit for this simulation= "+profit)
    //return
    log.debug("List of predicted prices ="+historyPoints)
    historyPoints
  }


  def endDataFrame(sparkSession: SparkSession, inputStock:DataFrame): DataFrame={
    //function which returns a dataframe of lists of price points for each simulation
    import sparkSession.implicits._
    var count = inputStock.count().toInt
    (0 to count).map(i=>getList(inputStock)).toDF()
  }

}
