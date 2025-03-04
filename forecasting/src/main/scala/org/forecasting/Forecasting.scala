package org.forecasting

/**
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

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.concurrent.TimeUnit

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import java.util.Properties
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Forecasting {

  val KAFKA_TOPIC_NAME = "test"
  val ZOOKEEPER_CONNECTION = "zookeeper:2181"
  val KAFKA_BOOTSTRAP_SERVER = "kafka:9093"

  val window = Time.of(60, TimeUnit.SECONDS)

  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    properties.setProperty("group.id", "forecasting")
    properties.setProperty("zookeeper.connect", ZOOKEEPER_CONNECTION)


    val kafkaConsumer = new FlinkKafkaConsumer[ObjectNode](
      KAFKA_TOPIC_NAME,
      KafkaJsonSchema,
      properties
    )

    // get input data
    val lines = env.addSource(kafkaConsumer)
                    .filter(!_.toString().equals("{}")) // filter incorrect values
                    .filter(_.get("measurement") != null)
                    .filter(_.get("measurement").asInt != 0)
    
    // execute and print result
    lines.print()

    val operations_minute = operationsPerMinute(lines, window)

    //lines.writeAsText("output.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    lines.map(x => x.toString).writeToSocket("localhost", 8080, new SimpleStringSchema())
    
    env.execute()

  }

  object KafkaJsonSchema extends SerializationSchema[ObjectNode] with DeserializationSchema[ObjectNode] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor
    import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

    override def serialize(t: ObjectNode): Array[Byte] = t.toString().getBytes("UTF-8")

    override def isEndOfStream(t: ObjectNode): Boolean = false

    override def deserialize(bytes: Array[Byte]): ObjectNode = {
      try {
        new ObjectMapper().readValue(new String(bytes, "UTF-8"), classOf[ObjectNode])
      } catch {
        case e:Exception => new ObjectMapper().readValue("{}", classOf[ObjectNode])
      }
    }
    override def getProducedType: TypeInformation[ObjectNode] = TypeExtractor.getForClass(classOf[ObjectNode])
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }

  def operationsPerMinute(lines: DataStream[ObjectNode], window: Time): DataStream[String] = {
    lines
      .map(json => (json, 1))
      .keyBy(jsonStr => {
        jsonStr._1.get("location_id")
        })
      .timeWindow(window)
      .sum(1)
      .map(x => x.toString)
  }
}