package com.quantifind.statsd

import com.timgroup.statsd.StatsDClientErrorHandler
import kafka.utils.Logging

class ErrorHandler(val handleIt:Exception => Unit) extends StatsDClientErrorHandler with Logging {

  override def handle(exception: Exception): Unit = {
    try {
      handleIt(exception)
    } catch {
      case ex:Throwable => logger.warn("Error sending statsd data:"+ex)
    }
  }
}
