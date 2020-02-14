import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.Random

//Util class created to get stock symbols and from those form the REST URL
// to be passed onto the driver or to download the files for reference.

object CSVUtil {

  val stockConfig = ConfigFactory.load("Symbols.conf")
  var stockSelection =  ListBuffer[String]()
  var urlList = ListBuffer[String]()
  val log: Logger = LoggerFactory.getLogger(CSVUtil.getClass)

  def listStocks():ListBuffer[String]={ //Parse the comma separated S&P 500 symbols and return a list of 5 random symbols
    val myList = stockConfig.getString("symbols")
    val stocks = myList.split(",")
    val stockList = ListBuffer[String]()
    val r = Random
    for(i<- 0 to 4){
      stockList += stocks(r.nextInt((stocks.length-1)))
      log.debug("Stock Seleted:"+stockList(i))
    }
    log.info("Stocks selected for simulation: "+stockList)
    stockList
  }

  def main(args: Array[String]): Unit = { // Run this to get the files downloaded to a specified directory
    import scala.io.Source

    val localPath="//home//aditya//Downloads//" //change to a path on your machine
    /*val urlPartStart ="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
    val urlPartEnd ="&outputsize=full&apikey=0C7HWW9RZQ5K0H1I&datatype=csv"*/

    val urlPartStart = stockConfig.getString("urlHead")
    val urlPartEnd = stockConfig.getString("urlTail")

    for(i<- 0 to 4){
      val url = urlPartStart+listStocks()(i)+urlPartEnd
      log.debug("Sending request to Vantage Point via:"+url)
      val html = Source.fromURL(url)
      val file = listStocks()(i)+".csv"
      Files.write(Paths.get(localPath+file), html.mkString.getBytes(StandardCharsets.UTF_8))
      stockSelection += file
      log.debug("Response from service dropped a file at:"+localPath+file)
    }
    log.info("CSV files of selected stocks: "+stockSelection)
  }

  def getUrls(): Unit = { //method to form URLs for each stock symbol
    /*val urlPartStart ="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
    val urlPartEnd ="&outputsize=compact&apikey=0C7HWW9RZQ5K0H1I&datatype=csv"*/
    val urlPartStart = stockConfig.getString("urlHead")
    val urlPartEnd = stockConfig.getString("urlTail")
    val key = stockConfig.getString("urlKey")

    val stocks = listStocks()
    for(i<- 0 to 4){
      val file = stocks(i)
      val url = urlPartStart+file+urlPartEnd+key
      stockSelection += file
      urlList += url
      log.debug("Forming URL for Stock:"+stockSelection(i)+" URL= "+urlList(i))
    }
    log.info("Stock portfolio formed="+stockSelection+" URLS for given stocks="+urlList)
  }

}
