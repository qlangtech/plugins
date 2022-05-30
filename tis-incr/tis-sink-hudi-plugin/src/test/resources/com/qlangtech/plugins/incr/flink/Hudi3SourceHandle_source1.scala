package com.qlangtech.tis.realtime.transfer.hudi

import java.util
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import com.qlangtech.tis.realtime.{HoodieFlinkSourceHandle}
import scala.collection.JavaConverters._
import org.apache.hudi.streamer.FlinkStreamerConfig
import org.apache.hudi.common.model.WriteOperationType
import com.qlangtech.tis.extension.TISExtension

import org.slf4j.LoggerFactory

@TISExtension()
class HudiSourceHandle extends HoodieFlinkSourceHandle {
  lazy val logger = LoggerFactory.getLogger( classOf[HudiSourceHandle])
  val _currVersion : Long = 1
override protected def createTabStreamerCfg(): java.util.Map[String , FlinkStreamerConfig] = {

  var cfgs: Map[String , FlinkStreamerConfig] = Map()
  cfgs.asJava
}

  def getVer() : Long = {
    _currVersion
  }

}
