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

  val AMEXStock = "AMEX-Stock.csv"
  val NYSEStock = "NYSE-Stock.csv"
  val NASDAQStock = "NASDAQ-Stock.csv"

  val AMEXStockOutput = "AMEXOutput"
  val NYSEStockOutput = "NYSEOutput"
  val NASDAQStockOutput = "NASDAQOutput"

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

  def startToPollAndSave(sc: SparkContext, inputFile: String, outputFile: String): Unit = {
    val data = sc.textFile(inputFile)
    val symbols = data.flatMap(parseLine).cache()

    println(inputFile + " Stocks Symbols: " + symbols.count())

    val stocks = symbols.flatMap(pollStock).filter(_.averVol > 0)

    val hotStocks = stocks.filter(stock => stock.volume/stock.averVol > 3).cache()

    println(inputFile + "Hot Stocks Amount: " + hotStocks.count())

    val savedFile = outputFile + new Date().getTime

    hotStocks.saveAsTextFile(savedFile)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "StockAnalysis")

    // Poll for NYSE
    startToPollAndSave(sc, NYSEStock, NYSEStockOutput)
    // Poll for NASDAQ
    startToPollAndSave(sc, NASDAQStock, NASDAQStockOutput)
  }

}
