package com.jebeljing.stock

/**
  * Created by jingshanyin on 10/11/17.
  */
trait StockData {

  def getInt(value: String): BigInt = {
    if (value.toString.trim.matches("N/A")) {
      0
    } else {
      BigInt(value)
    }
  }

  def getDouble(value: String): Double = {
    val str = value.toString.trim.replace("\"", "")
    if (str.length == 0 || str.equalsIgnoreCase("n/a")) {
      0.0
    } else {
      str.toDouble
    }
  }
}

class WrongIdException(msg: String) extends Throwable (msg)


