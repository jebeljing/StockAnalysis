package com.jebeljing.service

import java.io.File
import java.util.Date

import com.jebeljing.stock.{Stock, StockData}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by jingshanyin on 10/16/17.
  */
object StockPolling  extends StockData {

  def parseLine(line: String) : Option[(String, Double)] = {
    val fields = line.split("\",\"")
    if (fields.length >= 2) {
      if (fields(0).replace("\"","") == "Symbol") {
        None
      } else {
        val sale = getDouble(fields(2))
        if (sale > 0.0 && sale < 25.0) {
          Some((fields(0), sale))
        } else {
          None
        }
      }
    } else {
      None
    }
  }

  def pollStock(sym: (String, Double)) : Option[Stock] = {
    try {
      val stock = Stock(sym._1)
      Some(stock)
    } catch {
      case e: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val AMEXStock = "AMEX-Stock.csv"
    val NYSEStock = "NYSE-Stock.csv"
    val NASDAQStock = "NASDAQ-Stock.csv"

    val AMEXStockOutput = "AMEXOutput"
    val NYSEStockOutput = "NYSEOutput"
    val NASDAQStockOutput = "NASDAQOutput"

    val sc = new SparkContext("local[*]", "StockAnalysis")
    val data = sc.textFile(AMEXStock)
//    val data = sc.textFile(NYSEStock)
//    val data = sc.textFile(NASDAQStock)
    val symbols = data.flatMap(parseLine).cache()

    println("Stocks Symbols: " + symbols.count())

    val stocks = symbols.flatMap(pollStock).filter(_.averVol > 0)

    val hotStocks = stocks.filter(stock => stock.volume/stock.averVol > 3).cache()

    println("Hot Stocks Amount: " + hotStocks.count())

    val outputFile = AMEXStockOutput + new Date().getTime
//    val outputFile = NYSEStockOutput + new Date().getTime
//    val outputFile = NASDAQStockOutput + new Date().getTime

    hotStocks.saveAsTextFile(outputFile)
  }


}
