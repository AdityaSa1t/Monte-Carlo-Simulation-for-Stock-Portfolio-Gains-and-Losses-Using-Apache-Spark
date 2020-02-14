import org.slf4j.{Logger, LoggerFactory}

object Driver {

  val log: Logger = LoggerFactory.getLogger(Driver.getClass)

  def main(args: Array[String]): Unit = {

    //driver code which first calls a utility to return the list of stocks and their respective REST URLs

    CSVUtil.getUrls()
    val stocks = CSVUtil.stockSelection
    val urls = CSVUtil.urlList

    log.info("\n\nStock selection: "+stocks+"\n\n")

    log.debug("\n\nURLs for stocks:"+urls+"\n\n")

    //val outpath = "hdfs://localhost:9000/sparkOutput/StockProfit" //arg0

    val outPath = args(0)
    log.debug("Output of each simulation will be found at: "+outPath)

    for(i<-0 to 4){
      //Running a simulation for each stock selection
      val x = SingleSimulation
      log.info("For "+stocks(i)+" Making a call to: "+urls(i))
      x.run(outPath+stocks(i),urls(i))
    }

  }
}
