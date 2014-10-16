package com.quantifind.statsd

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import kafka.utils.Logging
import com.quantifind.sumac.FieldArgs

/** Enable writing of statsd counters for monitoring offsets **/
class StatsdWriter(val prefix:String, val statsdHost:String, val statsdPort:Int) extends Logging {

  import com.timgroup.statsd.NonBlockingStatsDClient

  val errHandler = new ErrorHandler(logger.warn)
  val statsDClient = new NonBlockingStatsDClient(prefix, statsdHost, statsdPort, errHandler)

  def writeMetric(timestamp:Long, offsetInfo:OffsetInfo):Unit = {

    val consumerGroupMetric:String = List(offsetInfo.topic, offsetInfo.partition, offsetInfo.group).mkString(".")
    val topicMetric:String = List(offsetInfo.topic, offsetInfo.partition).mkString(".")

    try {

      statsDClient.recordGaugeValue(consumerGroupMetric, offsetInfo.lag)
      statsDClient.recordGaugeValue(topicMetric, offsetInfo.logSize)
    } catch {
      case ex:Throwable => {
        this.logger.warn("Error sending statsd metrics: "+ex)
      }
    }
  }

}

object StatsdWriter {

  trait Arguments extends FieldArgs {

    // statsd integration
    var statsdEnabled: Boolean = false
    var statsdHost: String = "localhost"
    var statsdPort: Int = 5281
    var statsdPrefix: String = "offsetapp"
  }
}
