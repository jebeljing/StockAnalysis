package com.jebeljing.stock

import scala.io.Source

/**
  * Created by jingshanyin on 10/11/17.
  */
case class Stock(symbol: String, open: Double, low: Double, high: Double, low52wk: Double
                , high52wk: Double, volume: BigInt, averVol: BigInt)


object Stock extends StockData {

  val baseUrl = "http://download.finance.yahoo.com/d/quotes.csv?s="
  val queryParam = "&f=oghjkva2"

  def apply(symbol: String): Stock = {
    val validSym = symbol.replace("\"", "")
    val s = Source.fromURL(baseUrl + validSym + queryParam).mkString
    parseQuote(s, validSym)
  }

  def parseQuote(quote: String, symbol: String): Stock = {
    val info = quote.split(",")
    val open = getDouble(info(0))
    val low = getDouble(info(1))
    val high = getDouble(info(2))
    val low52wk = getDouble(info(3))
    val high52wk = getDouble(info(4))
    val volume = getInt(info(5))
    val dayAverVol = getInt(info(6).trim)
    val sym = symbol.toUpperCase

    new Stock(sym, open, low, high, low52wk, high52wk, volume, dayAverVol)
  }
}
