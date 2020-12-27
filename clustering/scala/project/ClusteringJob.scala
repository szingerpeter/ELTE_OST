/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package project

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.{Date, Properties}

object FraudDetectionJob {
    
    private val LOG = LoggerFactory.getLogger(FraudDetectionJob.getClass)
    
    @throws[Exception]
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        
        val kafkaConfig = new Properties()
        kafkaConfig.setProperty("bootstrap.servers", "127.0.0.1:9092")
        kafkaConfig.setProperty("group.id", "flink")
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1) // TODO: set to 8
        env.getConfig.setAutoWatermarkInterval(3600)
        
        val measurements = env.addSource(
            new FlinkKafkaConsumer[ObjectNode]("test", new InKafkaJsonSchema(), kafkaConfig) // TODO
        )
          .filter(!_.toString().equals("{}")) // filter incorrect values
          .filter(_.get("measurement") != null)
          .filter(_.get("measurement").asInt != 0)
          .map { item =>
            
              val time = new Date(item.get("timestamp").asLong())
              LOG.info(s"Time: ${time}")
              
              new Measurement(
                  timestamp = item.get("timestamp").asLong(),
                  location_id = item.get("location_id").asLong(),
                  measurement = item.get("measurement").asDouble()
              )
              
          }
          .name("Type conversion to measurements")
        
        val grouped = measurements
          .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Measurement] {
              override def checkAndGetNextWatermark(t: Measurement, l: Long): Watermark = new Watermark(t._timestamp)
              
              override def extractTimestamp(t: Measurement, l: Long): Long = t._timestamp
          })
          .name("Assign watermarks")
          //.keyBy { measurement =>
          //    measurement._timestamp
          //}
          .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10)))
          .allowedLateness(Time.milliseconds(0))
          .aggregate {
              new ListAggregateFunction[Measurement]
          }
          .name("Grouped by Days")
          //.map { measurementList => (measurementList.head._timestamp, measurementList.length) }.
          //.countWindowAll(5)
          //.aggregate {
          //    new ListAggregateFunction[List[Measurement]]
          //}
          .flatMap { (records : List[Measurement], collector : Collector[Map[Long, List[Measurement]]]) =>
              collector.collect(records.groupBy(_._location_id))
          }
          .name("Grouped by locations")
    
        //grouped
        //  .map { item => item.mapValues { list => list.length } }
        //  .name("Print lengths")
        //  .print()
        
        val asd = grouped
          .map { item => item.values.toList }
          
        asd
          .addSink(
              new FlinkKafkaProducer[List[List[Measurement]]](
                  "influxdb",
                  new OutKafkaJsonSchema,
                  kafkaConfig
              )
          )
          .name("InfluxDBSink")
        

        env.execute("Fraud Detection")
    }
}
