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

object ClusteringJob {
    
    private val LOG = LoggerFactory.getLogger(ClusteringJob.getClass)
    
    @throws[Exception]
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        
        val kafkaConfig = new Properties()
        kafkaConfig.setProperty("bootstrap.servers", "kafka:9093")
        kafkaConfig.setProperty("group.id", "flink")
        kafkaConfig.setProperty("zookeeper.connect", "zookeeper:2181")
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(2) // TODO: set to 8
        env.getConfig.setAutoWatermarkInterval(3600)
        
        val measurements = env.addSource(
            new FlinkKafkaConsumer[ObjectNode]("test", new InKafkaJsonSchema(), kafkaConfig) // TODO
        )
          .filter(!_.toString.equals("{}")) // filter incorrect values
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
        
        val dataStream = measurements
          .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Measurement] {
              override def checkAndGetNextWatermark(t: Measurement, l: Long): Watermark = new Watermark(t._timestamp)
              
              override def extractTimestamp(t: Measurement, l: Long): Long = t._timestamp
          })
          .name("Assign watermarks")
          .windowAll(TumblingEventTimeWindows.of(Time.hours(6)))
          .allowedLateness(Time.days(1))
          .aggregate {
              new ListAggregateFunction[Measurement]
          }
          .name("Grouped by Days")
          .flatMap { (records : List[Measurement], collector : Collector[Map[Long, List[Measurement]]]) =>
              collector.collect(records.groupBy(_._location_id))
          }
          .map { item =>
              item.mapValues { measurements =>
                  measurements.sortBy(_._timestamp)
              }
          }
          .name("Grouped by locations")
    
        val outStream = dataStream
          .keyBy(new JavaKeySelector[Map[Long, List[Measurement]], Long](value => 0))
          .process(new ProcessClustering)
          .name("Clustered measurements")

        val clusteringStream = outStream
          .keyBy(new JavaKeySelector[List[ClusteringResult], Long](value => 0))
          .process(new EvaluateClustering)
          .name("Evaluated clustering")

        val secondaryStream : DataStream[List[Measurement]] = dataStream
          .getSideOutput(new OutputTag[List[Measurement]]("Measurements"))
        
        
        clusteringStream
          .flatMap { (result: (List[ClusteringResult], Double), collector: Collector[List[Measurement]]) =>
              System.out.println(s"measurements: ${result._1}")
              System.out.println(s"clustering coefficient: ${result._2}")
              result._1.foreach(x => collector.collect(x.measurements))
          }
          .addSink(
              new FlinkKafkaProducer[List[Measurement]](
                  "influxdb",
                  new OutKafkaJsonSchema,
                  kafkaConfig
              )
          )
          .name("InfluxDBSink")


        env.execute("Clustering")
    }
}
