package project

import com.yahoo.labs.samoa.instances.{DenseInstance, Instance}
import moa.cluster.{Cluster, Clustering}
import moa.clusterers.denstream.MicroCluster
import moa.core.Measurement
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import java.util
import scala.collection.JavaConversions.asScalaBuffer

class ProcessClustering extends KeyedProcessFunction[DenStream, Map[Long, List[Measurement]], ClusteringResult] {
    
    type In = Map[Long, List[Measurement]]
    type Out = ClusteringResult
    
    lazy val state : ValueState[DenStream] = getRuntimeContext
      .getState(
          new ValueStateDescriptor[DenStream]("state", classOf[DenStream])
      )
    
    val sideOutputTag = OutputTag[String]("Clustering results")
    val inclusion_prob : Double = 0.75
    val FFT : FFT = new FFT()
    
    def preprocess(in : In): Map[Long, DenseInstance] = {
        
        in.map { location =>
        
            val id = location._1
            val measurements = location._2
            // TODO: Add fft code
            //val fft = this.FFT.fft(measurements)
            //    result.foreach { item =>
            //        println(item)
            //    }
            //}
        
            val values = measurements.map(_._measurement).toArray
            val instance = new DenseInstance(1.0D, values)
            (id, instance)
        }
    }
    
    def extractResults(input : Map[Long, DenseInstance], clustering : DenStream) : List[(Long, Long, MicroCluster)] = {
        
        val result = this
          .state
          .value()
          .getClusteringResult()
          .getClustering
          .asInstanceOf[util.ArrayList[MicroCluster]]
    
        input.map { location =>
        
            var label : Long = -1
            var cluster_info : MicroCluster = result.get(0)
            for (i <- result.indices) {
                val cluster: MicroCluster = result(i)
                if (cluster.getInclusionProbability(location._2) > inclusion_prob) {
                    label = i
                    cluster_info = cluster
                }
            }
            (location._1, label, cluster_info)
        }.toList
    }
    
    override def processElement(in : In, context : KeyedProcessFunction[DenStream, In, Out]#Context, collector : Collector[Out]) : Unit = {
        
        // TODO: convert elements with fft
        // TODO: create DenPoint instance
        // TODO: feed to Denstream
        // TODO: save & show output to secondary sink
        // TODO: extract results


        val clustering : DenStream = state.value()
        // TODO: reset Denstream object's state
        //clustering.resetLearningImpl()
        val input : Map[Long, DenseInstance] = preprocess(in)
    
        input.mapValues { instance =>
            clustering.trainOnInstanceImpl(instance)
        }
    
        this.state.update(clustering)
        extractResults(input, clustering).foreach { item =>
            val (location_id : Long, label : Long, info : MicroCluster) = item
            val measurements = in.get(location_id) match {
                case Some(value) => value
                case None => List()
            }
            
            collector.collect(
                new Out(location_id, label, measurements, info.asInstanceOf[ClusteringFeature])
            )
        }
        context.output(sideOutputTag, s"${this.state}")
    }
}
