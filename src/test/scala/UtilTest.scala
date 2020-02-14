import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.junit.Before
import org.junit

import scala.collection.mutable.ListBuffer

class UtilTest {

  private var sparkConfig: Config = null
  var url = new String
  var stocks = new ListBuffer[String]

  val spark = SparkSession.builder
    .master("local")
    .appName("SparkOne")
    .getOrCreate
  var sampleDataFrame = new Object
  val simulation = SingleSimulation


  @Before def getConfig: Unit = {
    CSVUtil.getUrls()
    sparkConfig = ConfigFactory.load("Symbols.conf")
    stocks= CSVUtil.stockSelection
  }

  @junit.Test
  def checkFileAndService: Unit = {
    val start = sparkConfig.getString("urlHead")
    val end = sparkConfig.getString("urlTail")
    val key = sparkConfig.getString("urlKey")
    url = start+stocks(0)+end+key
    assert(!url.isEmpty,"Required response didn't come through.")
  }

  @junit.Test
  def checkValidity:Unit ={
    assert((!url.toString.contains(stocks(0))),"The URL does not contain stock symbol, hence we wont get the required response.")
  }

  @junit.Test
  def checkList: Unit={
    assert(CSVUtil.listStocks().length<=5,"Didn't find the endpoint for all stocks")
  }

  @junit.Test
  def valiateStocks: Unit={
    var result = CSVUtil.stockSelection.size>0
    assert(result," Stock symbols not found.")
  }

  @junit.Test
  def valiateURLs: Unit={
    var result = CSVUtil.urlList.size>0
    assert(result," URLs not found.")
  }

}
